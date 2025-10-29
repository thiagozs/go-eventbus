package eventbus

import (
	"context"
	"fmt"
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
	process   func(ctx context.Context) error
}

// EventBus é o orquestrador central de eventos
type EventBus struct {
	handlers map[string][]func(ctx context.Context, payload any) error
	queue    chan eventWrapper
	workers  int
	wg       sync.WaitGroup
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	retries  int
	dlq      chan eventWrapper // Dead Letter Queue
}

// Config para configurar o EventBus
type Config struct {
	Workers       int           // Número de workers no pool
	QueueSize     int           // Tamanho do buffer da queue
	Retries       int           // Tentativas de reprocessamento
	DLQSize       int           // Tamanho da Dead Letter Queue
	WorkerTimeout time.Duration // Timeout por evento
}

// NewEventBus cria uma nova instância do EventBus
func NewEventBus(config Config) *EventBus {
	if config.Workers <= 0 {
		config.Workers = 10
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 100
	}
	if config.Retries <= 0 {
		config.Retries = 3
	}
	if config.DLQSize <= 0 {
		config.DLQSize = 50
	}
	if config.WorkerTimeout <= 0 {
		config.WorkerTimeout = 30 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	bus := &EventBus{
		handlers: make(map[string][]func(ctx context.Context, payload any) error),
		queue:    make(chan eventWrapper, config.QueueSize),
		workers:  config.Workers,
		ctx:      ctx,
		cancel:   cancel,
		retries:  config.Retries,
		dlq:      make(chan eventWrapper, config.DLQSize),
	}

	// Inicia os workers
	bus.startWorkers()

	return bus
}

// Subscribe registra um handler tipado para um tipo de evento
func Subscribe[T any](eb *EventBus, eventType string, handler Handler[T]) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	// Wrapper que faz o type assertion e chama o handler tipado
	wrappedHandler := func(ctx context.Context, payload any) error {
		typedPayload, ok := payload.(T)
		if !ok {
			return fmt.Errorf("falha ao converter payload para o tipo esperado")
		}

		event := Event[T]{
			Type:      eventType,
			Payload:   typedPayload,
			Timestamp: time.Now(),
		}

		return handler(ctx, event)
	}

	eb.handlers[eventType] = append(eb.handlers[eventType], wrappedHandler)
}

// Publish envia um evento tipado para a queue (não bloqueante)
func Publish[T any](eb *EventBus, eventType string, payload T) error {
	return PublishWithPriority(eb, eventType, payload, 0)
}

// PublishWithPriority envia um evento tipado com prioridade
func PublishWithPriority[T any](eb *EventBus, eventType string,
	payload T, priority int) error {
	wrapper := eventWrapper{
		eventType: eventType,
		payload:   payload,
		timestamp: time.Now(),
		priority:  priority,
		process: func(ctx context.Context) error {
			eb.mu.RLock()
			handlers, exists := eb.handlers[eventType]
			eb.mu.RUnlock()

			if !exists || len(handlers) == 0 {
				return fmt.Errorf("nenhum handler registrado para evento: %s", eventType)
			}

			for _, handler := range handlers {
				if err := handler(ctx, payload); err != nil {
					return err
				}
			}
			return nil
		},
	}

	select {
	case eb.queue <- wrapper:
		return nil
	case <-eb.ctx.Done():
		return fmt.Errorf("eventbus encerrado")
	default:
		return fmt.Errorf("queue cheia, evento rejeitado")
	}
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
		select {
		case wrapper := <-eb.queue:
			eb.processEventWrapper(wrapper, 0)
		case <-eb.ctx.Done():
			return
		}
	}
}

// processEventWrapper processa um evento com retry logic
func (eb *EventBus) processEventWrapper(wrapper eventWrapper, attempt int) {
	ctx, cancel := context.WithTimeout(eb.ctx, 30*time.Second)
	err := wrapper.process(ctx)
	cancel()

	if err != nil {
		fmt.Printf("[ERROR] Falha ao processar evento %s (tentativa %d/%d): %v\n",
			wrapper.eventType, attempt+1, eb.retries+1, err)

		// Retry logic - tenta mais vezes se ainda não atingiu o limite
		if attempt < eb.retries {
			time.Sleep(time.Duration(attempt+1) * time.Second) // Backoff exponencial simples
			eb.processEventWrapper(wrapper, attempt+1)
			return
		}

		// Envia para DLQ se todas as tentativas falharam
		select {
		case eb.dlq <- wrapper:
			fmt.Printf("[DLQ] Evento %s enviado para Dead Letter Queue\n", wrapper.eventType)
		default:
			fmt.Printf("[ERROR] DLQ cheia, evento %s perdido!\n", wrapper.eventType)
		}
	}
}

// GetDLQ retorna o canal da Dead Letter Queue (somente leitura)
func (eb *EventBus) GetDLQ() <-chan eventWrapper {
	return eb.dlq
}

// DLQEvent representa um evento na DLQ com informações básicas
type DLQEvent struct {
	Type      string
	Payload   any
	Timestamp time.Time
}

// GetDLQEvent converte um eventWrapper em DLQEvent para acesso externo
func GetDLQEvent(wrapper eventWrapper) DLQEvent {
	return DLQEvent{
		Type:      wrapper.eventType,
		Payload:   wrapper.payload,
		Timestamp: wrapper.timestamp,
	}
}

// Shutdown encerra o EventBus gracefully
func (eb *EventBus) Shutdown(timeout time.Duration) error {
	fmt.Println("[INFO] Iniciando shutdown do EventBus...")

	// Para de aceitar novos eventos
	eb.cancel()

	// Aguarda workers terminarem ou timeout
	done := make(chan struct{})
	go func() {
		eb.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("[INFO] EventBus encerrado com sucesso")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout ao encerrar EventBus")
	}
}

// Stats retorna estatísticas do EventBus
func (eb *EventBus) Stats() map[string]any {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	handlerCount := 0
	for _, handlers := range eb.handlers {
		handlerCount += len(handlers)
	}

	return map[string]any{
		"handlers_registrados": handlerCount,
		"eventos_tipos":        len(eb.handlers),
		"queue_size":           len(eb.queue),
		"dlq_size":             len(eb.dlq),
		"workers":              eb.workers,
	}
}
