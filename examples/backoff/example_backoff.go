package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thiagozs/go-eventbus"
)

type FlakyEvent struct {
	ID string
}

func main() {
	// Backoff linear de 200ms por tentativa, limitado a 500ms (mais jitter de até 20%):
	// esperas aproximadas de 200ms, 400ms, 500ms, 500ms...
	bus := eventbus.NewEventBus(eventbus.Config{
		Workers:      2,
		Retries:      4,
		RetryBackoff: 200 * time.Millisecond,
		MaxBackoff:   500 * time.Millisecond,
	})

	start := time.Now()

	// Falha nas 3 primeiras tentativas e funciona na 4ª
	attempts := 0
	eventbus.Subscribe(bus, "flaky.recovers", func(ctx context.Context, e eventbus.Event[FlakyEvent]) error {
		attempts++
		fmt.Printf("[%v] flaky.recovers tentativa %d\n", time.Since(start).Round(time.Millisecond), attempts)
		if attempts < 4 {
			return errors.New("falha temporária")
		}
		fmt.Println("✅ recuperou")
		return nil
	})

	// Sempre falha: esgota as tentativas e vai para a DLQ
	failures := 0
	eventbus.Subscribe(bus, "flaky.dead", func(ctx context.Context, e eventbus.Event[FlakyEvent]) error {
		failures++
		fmt.Printf("[%v] flaky.dead tentativa %d\n", time.Since(start).Round(time.Millisecond), failures)
		return errors.New("falha permanente")
	})

	eventbus.Publish(bus, "flaky.recovers", FlakyEvent{ID: "1"})
	eventbus.Publish(bus, "flaky.dead", FlakyEvent{ID: "2"})

	select {
	case d := <-bus.GetDLQ():
		fmt.Printf("💀 DLQ: %s após %d tentativas: %v\n", d.Type, d.Attempts, d.Err)
	case <-time.After(10 * time.Second):
		fmt.Println("timeout aguardando a DLQ")
	}

	if err := bus.Shutdown(5 * time.Second); err != nil {
		fmt.Println("shutdown:", err)
	}
}
