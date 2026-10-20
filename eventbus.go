package eventbus

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Event representa um evento genérico com payload tipado
type Event[T any] struct {
	Type      string
	Payload   T
	Timestamp time.Time
	Priority  int // 0 = normal, valores maiores = maior prioridade
}

// Handler é a função que processa eventos tipados
type Handler[T any] func(ctx context.Context, event Event[T]) error

// eventWrapper encapsula eventos de qualquer tipo para a queue interna
type eventWrapper struct {
	eventType string
	payload   any
	timestamp time.Time
	priority  int
	seq       uint64 // ordem de publicação, desempata prioridades iguais
	resumeID  uint64 // retries retomam a partir do handler que falhou
}

// handlerFunc é um handler com o tipo já resolvido
type handlerFunc func(ctx context.Context, w eventWrapper) error

type handlerEntry struct {
	id uint64
	fn handlerFunc
}

// priorityQueue é uma heap: maior prioridade primeiro, FIFO dentro da mesma prioridade
type priorityQueue []eventWrapper

func (q priorityQueue) Len() int { return len(q) }
func (q priorityQueue) Less(i, j int) bool {
	if q[i].priority != q[j].priority {
		return q[i].priority > q[j].priority
	}
	return q[i].seq < q[j].seq
}
func (q priorityQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *priorityQueue) Push(x any)   { *q = append(*q, x.(eventWrapper)) }
func (q *priorityQueue) Pop() any {
	old := *q
	n := len(old)
	w := old[n-1]
	*q = old[:n-1]
	return w
}

// EventBus é o orquestrador central de eventos
type EventBus struct {
	handlers map[string][]handlerEntry
	nextID   uint64
	mu       sync.RWMutex

	qmu      sync.Mutex
	qcond    *sync.Cond
	queue    priorityQueue
	capacity int
	closed   bool
	seq      uint64

	workers int
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	retries int
	timeout time.Duration
	dlq     chan DLQEvent // Dead Letter Queue
	log     *slog.Logger
}

// Config para configurar o EventBus
type Config struct {
	Workers       int           // Número de workers no pool (default: 10)
	QueueSize     int           // Capacidade da queue (default: 100)
	Retries       int           // Retries extras após falha (default: 3; valor negativo desativa)
	DLQSize       int           // Tamanho da Dead Letter Queue (default: 50)
	WorkerTimeout time.Duration // Timeout por tentativa de processamento (default: 30s)
	Logger        *slog.Logger  // Logger (default: slog.Default())
}

// NewEventBus cria uma nova instância do EventBus
func NewEventBus(config Config) *EventBus {
	if config.Workers <= 0 {
		config.Workers = 10
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 100
	}
	if config.Retries == 0 {
		config.Retries = 3
	} else if config.Retries < 0 {
		config.Retries = 0
	}
	if config.DLQSize <= 0 {
		config.DLQSize = 50
	}
	if config.WorkerTimeout <= 0 {
		config.WorkerTimeout = 30 * time.Second
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	bus := &EventBus{
		handlers: make(map[string][]handlerEntry),
		capacity: config.QueueSize,
		workers:  config.Workers,
		ctx:      ctx,
		cancel:   cancel,
		retries:  config.Retries,
		timeout:  config.WorkerTimeout,
		dlq:      make(chan DLQEvent, config.DLQSize),
		log:      config.Logger,
	}
	bus.qcond = sync.NewCond(&bus.qmu)

	// Acorda os workers quando o contexto é cancelado (shutdown forçado)
	context.AfterFunc(ctx, func() {
		bus.qmu.Lock()
		bus.qcond.Broadcast()
		bus.qmu.Unlock()
	})

	bus.startWorkers()

	return bus
}

// Subscribe registra um handler tipado para um tipo de evento.
// Retorna uma função que remove o registro do handler.
func Subscribe[T any](eb *EventBus, eventType string, handler Handler[T]) (unsubscribe func()) {
	// Wrapper que faz o type assertion e chama o handler tipado
	wrappedHandler := func(ctx context.Context, w eventWrapper) error {
		typedPayload, ok := w.payload.(T)
		if !ok {
			return fmt.Errorf("falha ao converter payload para o tipo esperado")
		}

		return handler(ctx, Event[T]{
			Type:      eventType,
			Payload:   typedPayload,
			Timestamp: w.timestamp,
			Priority:  w.priority,
		})
	}

	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.nextID++
	id := eb.nextID
	eb.handlers[eventType] = append(eb.handlers[eventType], handlerEntry{id: id, fn: wrappedHandler})

	return func() {
		eb.mu.Lock()
		defer eb.mu.Unlock()

		entries := eb.handlers[eventType]
		for i, e := range entries {
			if e.id == id {
				eb.handlers[eventType] = append(entries[:i:i], entries[i+1:]...)
				break
			}
		}
		if len(eb.handlers[eventType]) == 0 {
			delete(eb.handlers, eventType)
		}
	}
}

// Publish envia um evento tipado para a queue (não bloqueante)
func Publish[T any](eb *EventBus, eventType string, payload T) error {
	return PublishWithPriority(eb, eventType, payload, 0)
}

// PublishWithPriority envia um evento tipado com prioridade.
// Eventos de maior prioridade são processados primeiro.
func PublishWithPriority[T any](eb *EventBus, eventType string,
	payload T, priority int) error {
	return eb.enqueue(eventWrapper{
		eventType: eventType,
		payload:   payload,
		timestamp: time.Now(),
		priority:  priority,
	})
}

func (eb *EventBus) enqueue(w eventWrapper) error {
	eb.qmu.Lock()
	defer eb.qmu.Unlock()

	if eb.closed {
		return errors.New("eventbus encerrado")
	}
	if len(eb.queue) >= eb.capacity {
		return errors.New("queue cheia, evento rejeitado")
	}

	eb.seq++
	w.seq = eb.seq
	heap.Push(&eb.queue, w)
	eb.qcond.Signal()
	return nil
}

// dequeue bloqueia até haver evento. Retorna false quando o bus foi
// encerrado e a queue está vazia, ou quando o shutdown foi forçado.
func (eb *EventBus) dequeue() (eventWrapper, bool) {
	eb.qmu.Lock()
	defer eb.qmu.Unlock()

	for len(eb.queue) == 0 && !eb.closed && eb.ctx.Err() == nil {
		eb.qcond.Wait()
	}
	if eb.ctx.Err() != nil || len(eb.queue) == 0 {
		return eventWrapper{}, false
	}
	return heap.Pop(&eb.queue).(eventWrapper), true
}

// startWorkers inicia o pool de workers
func (eb *EventBus) startWorkers() {
	for i := 0; i < eb.workers; i++ {
		eb.wg.Add(1)
		go eb.worker()
	}
}

// worker processa eventos da queue
func (eb *EventBus) worker() {
	defer eb.wg.Done()

	for {
		wrapper, ok := eb.dequeue()
		if !ok {
			return
		}
		eb.processEventWrapper(wrapper)
	}
}

// dispatch executa os handlers do evento em ordem. Em caso de falha,
// guarda em w.resumeID o handler que falhou para que o retry não
// reexecute os que já tiveram sucesso.
func (eb *EventBus) dispatch(ctx context.Context, w *eventWrapper) error {
	eb.mu.RLock()
	entries := append([]handlerEntry(nil), eb.handlers[w.eventType]...)
	eb.mu.RUnlock()

	if len(entries) == 0 {
		return fmt.Errorf("nenhum handler registrado para evento: %s", w.eventType)
	}

	for _, e := range entries {
		if e.id < w.resumeID {
			continue
		}
		if err := e.fn(ctx, *w); err != nil {
			w.resumeID = e.id
			return err
		}
	}
	return nil
}

// processEventWrapper processa um evento com retry e backoff linear
func (eb *EventBus) processEventWrapper(wrapper eventWrapper) {
	var err error
	for attempt := 0; attempt <= eb.retries; attempt++ {
		ctx, cancel := context.WithTimeout(eb.ctx, eb.timeout)
		err = eb.dispatch(ctx, &wrapper)
		cancel()

		if err == nil {
			return
		}

		eb.log.Error("falha ao processar evento",
			"type", wrapper.eventType, "attempt", attempt+1,
			"max_attempts", eb.retries+1, "error", err)

		if attempt == eb.retries {
			break
		}

		// Backoff interrompido pelo shutdown forçado
		select {
		case <-time.After(time.Duration(attempt+1) * time.Second):
		case <-eb.ctx.Done():
			return
		}
	}

	// Envia para DLQ se todas as tentativas falharam
	select {
	case eb.dlq <- DLQEvent{
		Type:      wrapper.eventType,
		Payload:   wrapper.payload,
		Timestamp: wrapper.timestamp,
		Err:       err,
		Attempts:  eb.retries + 1,
	}:
		eb.log.Warn("evento enviado para a DLQ", "type", wrapper.eventType)
	default:
		eb.log.Error("DLQ cheia, evento perdido", "type", wrapper.eventType)
	}
}

// DLQEvent representa um evento que esgotou as tentativas de processamento
type DLQEvent struct {
	Type      string
	Payload   any
	Timestamp time.Time
	Err       error // último erro retornado
	Attempts  int   // total de tentativas realizadas
}

// GetDLQ retorna o canal da Dead Letter Queue (somente leitura)
func (eb *EventBus) GetDLQ() <-chan DLQEvent {
	return eb.dlq
}

// Shutdown encerra o EventBus gracefully: deixa de aceitar eventos,
// processa os pendentes e aguarda os workers até o timeout. Se o
// timeout estourar, os handlers em execução são cancelados.
func (eb *EventBus) Shutdown(timeout time.Duration) error {
	eb.log.Info("iniciando shutdown do EventBus")

	eb.qmu.Lock()
	eb.closed = true
	eb.qcond.Broadcast()
	eb.qmu.Unlock()

	done := make(chan struct{})
	go func() {
		eb.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		eb.cancel()
		eb.log.Info("EventBus encerrado com sucesso")
		return nil
	case <-timer.C:
		eb.cancel()
		return errors.New("timeout ao encerrar EventBus")
	}
}

// Stats retorna estatísticas do EventBus
func (eb *EventBus) Stats() map[string]any {
	eb.mu.RLock()
	handlerCount := 0
	for _, handlers := range eb.handlers {
		handlerCount += len(handlers)
	}
	typesCount := len(eb.handlers)
	eb.mu.RUnlock()

	eb.qmu.Lock()
	queueSize := len(eb.queue)
	eb.qmu.Unlock()

	return map[string]any{
		"handlers_registrados": handlerCount,
		"eventos_tipos":        typesCount,
		"queue_size":           queueSize,
		"dlq_size":             len(eb.dlq),
		"workers":              eb.workers,
	}
}
