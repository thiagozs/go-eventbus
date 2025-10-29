package eventbus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Test structs
type TestEvent struct {
	Message string
	Value   int
}

type MultiEvent struct {
	ID string
}

func TestNewEventBus(t *testing.T) {
	config := Config{
		Workers:   5,
		QueueSize: 50,
		Retries:   3,
	}

	bus := NewEventBus(config)

	if bus == nil {
		t.Fatal("EventBus não deveria ser nil")
	}

	if bus.workers != 5 {
		t.Errorf("Esperado 5 workers, got %d", bus.workers)
	}

	bus.Shutdown(1 * time.Second)
}

func TestSubscribeAndPublish(t *testing.T) {
	bus := NewEventBus(Config{Workers: 2, QueueSize: 10})
	defer bus.Shutdown(2 * time.Second)

	var wg sync.WaitGroup
	wg.Add(1)

	received := false
	Subscribe(bus, "test.event", func(ctx context.Context, event Event[TestEvent]) error {
		received = true
		if event.Type != "test.event" {
			t.Errorf("Esperado type 'test.event', got '%s'", event.Type)
		}
		if event.Payload.Message != "hello" {
			t.Errorf("Esperado message 'hello', got '%s'", event.Payload.Message)
		}
		wg.Done()
		return nil
	})

	err := Publish(bus, "test.event", TestEvent{Message: "hello", Value: 42})
	if err != nil {
		t.Fatalf("Erro ao publicar: %v", err)
	}

	wg.Wait()

	if !received {
		t.Error("Handler não foi executado")
	}
}

func TestMultipleHandlers(t *testing.T) {
	bus := NewEventBus(Config{Workers: 2, QueueSize: 10})
	defer bus.Shutdown(2 * time.Second)

	var wg sync.WaitGroup
	wg.Add(2)

	count := 0
	var mu sync.Mutex

	handler := func(ctx context.Context, event Event[MultiEvent]) error {
		mu.Lock()
		count++
		mu.Unlock()
		wg.Done()
		return nil
	}

	Subscribe(bus, "multi.event", handler)
	Subscribe(bus, "multi.event", handler)

	Publish(bus, "multi.event", MultiEvent{ID: "123"})

	wg.Wait()

	if count != 2 {
		t.Errorf("Esperado 2 handlers executados, got %d", count)
	}
}

func TestRetryLogic(t *testing.T) {
	bus := NewEventBus(Config{
		Workers:   1,
		QueueSize: 10,
		Retries:   2, // Vai tentar: 1 inicial + 2 retries = 3 total
	})
	defer bus.Shutdown(3 * time.Second)

	attempts := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)

	Subscribe(bus, "retry.event", func(ctx context.Context, event Event[TestEvent]) error {
		mu.Lock()
		attempts++
		isLast := attempts == 3
		mu.Unlock()

		if isLast {
			wg.Done()
		}
		return errors.New("simulated error")
	})

	Publish(bus, "retry.event", TestEvent{Message: "test"})

	// Aguarda completar todas as tentativas
	wg.Wait()

	mu.Lock()
	finalAttempts := attempts
	mu.Unlock()

	// Config.Retries = 2 significa: 1 tentativa inicial + 2 retries = 3 total
	expected := 3
	if finalAttempts != expected {
		t.Errorf("Esperado exatamente %d tentativas, got %d", expected, finalAttempts)
	}
}

func TestDeadLetterQueue(t *testing.T) {
	bus := NewEventBus(Config{
		Workers:   1,
		QueueSize: 10,
		Retries:   1, // 1 inicial + 1 retry = 2 tentativas
		DLQSize:   5,
	})
	defer bus.Shutdown(3 * time.Second)

	var wg sync.WaitGroup
	wg.Add(1)

	attempts := 0
	var mu sync.Mutex

	// Handler que sempre falha
	Subscribe(bus, "dlq.event", func(ctx context.Context, event Event[TestEvent]) error {
		mu.Lock()
		attempts++
		mu.Unlock()
		return errors.New("always fails")
	})

	// Monitora DLQ
	go func() {
		dlq := bus.GetDLQ()
		wrapper := <-dlq
		dlqEvent := GetDLQEvent(wrapper)
		if dlqEvent.Type != "dlq.event" {
			t.Errorf("Esperado evento 'dlq.event' na DLQ, got '%s'", dlqEvent.Type)
		}

		// Verifica que tentou o número correto de vezes
		mu.Lock()
		finalAttempts := attempts
		mu.Unlock()

		if finalAttempts != 2 {
			t.Errorf("Esperado 2 tentativas antes da DLQ, got %d", finalAttempts)
		}

		wg.Done()
	}()

	Publish(bus, "dlq.event", TestEvent{Message: "fail"})

	wg.Wait()
}

func TestPriorityEvents(t *testing.T) {
	bus := NewEventBus(Config{Workers: 1, QueueSize: 10})
	defer bus.Shutdown(2 * time.Second)

	var wg sync.WaitGroup
	wg.Add(2)

	// Registra handlers para os eventos
	Subscribe(bus, "high.priority", func(ctx context.Context, event Event[TestEvent]) error {
		wg.Done()
		return nil
	})

	Subscribe(bus, "normal.priority", func(ctx context.Context, event Event[TestEvent]) error {
		wg.Done()
		return nil
	})

	err := PublishWithPriority(bus, "high.priority", TestEvent{Message: "important"}, 10)
	if err != nil {
		t.Fatalf("Erro ao publicar evento com prioridade: %v", err)
	}

	err = Publish(bus, "normal.priority", TestEvent{Message: "regular"})
	if err != nil {
		t.Fatalf("Erro ao publicar evento normal: %v", err)
	}

	wg.Wait()
}

func TestStats(t *testing.T) {
	bus := NewEventBus(Config{Workers: 5, QueueSize: 100})
	defer bus.Shutdown(1 * time.Second)

	Subscribe(bus, "event1", func(ctx context.Context, event Event[TestEvent]) error { return nil })
	Subscribe(bus, "event2", func(ctx context.Context, event Event[TestEvent]) error { return nil })
	Subscribe(bus, "event2", func(ctx context.Context, event Event[TestEvent]) error { return nil })

	stats := bus.Stats()

	if stats["workers"] != 5 {
		t.Errorf("Esperado 5 workers, got %v", stats["workers"])
	}

	if stats["eventos_tipos"] != 2 {
		t.Errorf("Esperado 2 tipos de eventos, got %v", stats["eventos_tipos"])
	}

	if stats["handlers_registrados"] != 3 {
		t.Errorf("Esperado 3 handlers, got %v", stats["handlers_registrados"])
	}
}

func TestGracefulShutdown(t *testing.T) {
	bus := NewEventBus(Config{Workers: 2, QueueSize: 10})

	var wg sync.WaitGroup
	wg.Add(1)

	Subscribe(bus, "shutdown.event", func(ctx context.Context, event Event[TestEvent]) error {
		time.Sleep(500 * time.Millisecond)
		wg.Done()
		return nil
	})

	Publish(bus, "shutdown.event", TestEvent{Message: "test"})

	err := bus.Shutdown(2 * time.Second)
	if err != nil {
		t.Errorf("Erro no shutdown: %v", err)
	}

	wg.Wait()
}

func TestQueueFull(t *testing.T) {
	bus := NewEventBus(Config{
		Workers:   1, // Precisa de pelo menos 1 worker
		QueueSize: 2,
		Retries:   0, // Sem retries
	})

	// Registra um handler que bloqueia para manter a queue cheia
	blockChan := make(chan struct{})
	Subscribe(bus, "event", func(ctx context.Context,
		event Event[TestEvent]) error {
		<-blockChan // Bloqueia até liberarmos
		return nil
	})

	// Publica eventos para encher a queue
	// Worker vai pegar o primeiro e bloquear
	// Os próximos 2 vão encher o buffer da queue (size=2)
	err1 := Publish(bus, "event", TestEvent{Message: "1"})
	if err1 != nil {
		t.Fatalf("Erro ao publicar primeiro evento: %v", err1)
	}

	// Aguarda um pouco para o worker pegar o primeiro
	time.Sleep(50 * time.Millisecond)

	err2 := Publish(bus, "event", TestEvent{Message: "2"})
	if err2 != nil {
		t.Fatalf("Erro ao publicar segundo evento: %v", err2)
	}

	err3 := Publish(bus, "event", TestEvent{Message: "3"})
	if err3 != nil {
		t.Fatalf("Erro ao publicar terceiro evento: %v", err3)
	}

	// Agora a queue está cheia (worker bloqueado + 2 no buffer)
	// Este deve falhar
	err := Publish(bus, "event", TestEvent{Message: "4"})
	if err == nil {
		t.Error("Esperado erro ao publicar com queue cheia")
	}

	// Libera o bloqueio e faz shutdown
	close(blockChan)
	bus.Shutdown(2 * time.Second)
}

func BenchmarkPublish(b *testing.B) {
	bus := NewEventBus(Config{Workers: 10, QueueSize: 1000})
	defer bus.Shutdown(5 * time.Second)

	Subscribe(bus, "bench.event", func(ctx context.Context,
		event Event[TestEvent]) error {
		return nil
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Publish(bus, "bench.event", TestEvent{Message: "bench", Value: i})
	}
}

func BenchmarkConcurrentPublish(b *testing.B) {
	bus := NewEventBus(Config{Workers: 10, QueueSize: 10000})
	defer bus.Shutdown(5 * time.Second)

	Subscribe(bus, "bench.event", func(ctx context.Context,
		event Event[TestEvent]) error {
		return nil
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			Publish(bus, "bench.event", TestEvent{Message: "bench", Value: i})
			i++
		}
	})
}
