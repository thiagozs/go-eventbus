package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thiagozs/go-eventbus"
)

type OrderCreated struct {
	ID string
}

// outbox simula a tabela de outbox do PostgreSQL: é ela quem decide quando repetir.
type outbox struct {
	mu     sync.Mutex
	status map[string]string
}

func (o *outbox) mark(id, status string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.status[id] = status
}

func main() {
	ob := &outbox{status: map[string]string{}}

	// O barramento é só um pool de execução: sem retry interno e sem DLQ.
	// Cada falha definitiva é devolvida ao outbox via OnFailure.
	bus := eventbus.NewEventBus(eventbus.Config{
		Workers: 2,
		Retries: -1,
		DLQSize: -1,
		OnFailure: func(d eventbus.DLQEvent) {
			ev := d.Payload.(OrderCreated)
			fmt.Printf("❌ %s falhou (%v): outbox agenda novo retry\n", ev.ID, d.Err)
			ob.mark(ev.ID, "retry_agendado")
		},
	})

	var wg sync.WaitGroup
	eventbus.Subscribe(bus, "order.created", func(ctx context.Context, e eventbus.Event[OrderCreated]) error {
		defer wg.Done()
		if e.Payload.ID == "order-2" {
			return errors.New("gateway indisponível")
		}
		fmt.Printf("✅ %s processado\n", e.Payload.ID)
		ob.mark(e.Payload.ID, "concluido")
		return nil
	})

	for _, id := range []string{"order-1", "order-2", "order-3"} {
		wg.Add(1)
		eventbus.Publish(bus, "order.created", OrderCreated{ID: id})
	}
	wg.Wait()

	if err := bus.Shutdown(5 * time.Second); err != nil {
		fmt.Println("shutdown:", err)
	}

	fmt.Println("DLQ nil:", bus.GetDLQ() == nil)
	fmt.Println("dlq_dropped:", bus.Stats()["dlq_dropped"])
	ob.mu.Lock()
	for _, id := range []string{"order-1", "order-2", "order-3"} {
		fmt.Printf("outbox[%s] = %s\n", id, ob.status[id])
	}
	ob.mu.Unlock()
}
