# go-eventbus - Biblioteca de Eventos

Uma biblioteca leve e eficiente para orquestração de eventos assíncronos em Golang.

## Features

- ✅ **Type-Safe com Generics** - Zero type assertions, compile-time safety!
- ✅ **Worker Pool** - Pool configurável de goroutines para processar eventos
- ✅ **Event Queue** - Fila interna limitada, ordenada por prioridade (heap)
- ✅ **Retry Logic** - Reprocessamento automático em caso de falha
- ✅ **Dead Letter Queue (DLQ)** - Eventos que falharam após todas as tentativas
- ✅ **Priority Events** - Maior prioridade é processada primeiro, FIFO entre prioridades iguais
- ✅ **Graceful Shutdown** - Encerramento seguro aguardando processar eventos pendentes
- ✅ **Thread-Safe** - Totalmente seguro para uso concorrente
- ✅ **Context Support** - Timeout e cancelamento via context

## Instalação

Requer Go 1.27.0 ou superior.

```bash
go get github.com/thiagozs/go-eventbus
```

## Uso Básico

```go
package main

import (
    "context"
    "fmt"
    "eventbus"
)

// Define suas structs tipadas
type UserCreatedEvent struct {
    UserID string
    Email  string
    Name   string
}

func main() {
    // Configuração
    config := eventbus.Config{
        Workers:   5,
        QueueSize: 100,
        Retries:   3,
    }

    bus := eventbus.NewEventBus(config)

    // Registra handler TIPADO - sem type assertions!
    eventbus.Subscribe(bus, "user.created", func(ctx context.Context, 
        event eventbus.Event[UserCreatedEvent]) error {
        // Acesso direto aos campos tipados
        fmt.Printf("Novo usuário: %s (%s)\n", event.Payload.Name, event.Payload.Email)
        return nil
    })

    // Publica evento com struct tipada
    eventbus.Publish(bus, "user.created", UserCreatedEvent{
        UserID: "123",
        Email:  "joao@example.com",
        Name:   "João Silva",
    })

    // Shutdown graceful
    bus.Shutdown(5 * time.Second)
}
```

## Configuração

```go
type Config struct {
    Workers       int           // Número de workers (default: 10)
    QueueSize     int           // Tamanho do buffer (default: 100)
    Retries       int           // Tentativas extras após falha (default: 3)
                                 // Total = 1 inicial + Retries
                                 // Retries < 0 desativa os retries; 0 usa o default (3)
                                 // Ex: Retries=3 → 4 tentativas no total
    DLQSize       int           // Tamanho da DLQ (default: 50)
    WorkerTimeout time.Duration // Timeout por tentativa (default: 30s)
    RetryBackoff  time.Duration // Base do backoff linear (default: 1s)
    MaxBackoff    time.Duration // Teto do backoff, antes do jitter de até 20% (default: 30s)
    OnDLQDrop     func(DLQEvent) // Chamado quando a DLQ está cheia (opcional)
    OnFailure     func(DLQEvent) // Chamado quando o evento esgota as tentativas, com ou sem DLQ (opcional)
    Logger        *slog.Logger  // Logger (default: slog.Default())
}
```

## Exemplos Avançados

### Por que usar Generics?

**Antes (sem generics):**

```go
bus.Subscribe("user.created", func(ctx context.Context, 
    event eventbus.Event) error {
    // Precisa fazer type assertion manual
    payload := event.Payload.(map[string]interface{})
    email := payload["email"].(string) // Pode quebrar em runtime!
    return nil
})
```

**Agora (com generics):**

```go
type UserCreatedEvent struct {
    Email string
    Name  string
}

eventbus.Subscribe(bus, "user.created", func(ctx context.Context, 
    event eventbus.Event[UserCreatedEvent]) error {
    // Type-safe! Autocomplete funciona!
    email := event.Payload.Email // Compile-time safety!
    return nil
})
```

### Eventos com Prioridade

```go
// Evento normal (prioridade 0)
eventbus.Publish(bus, "notification.email", 
EmailData{To: "user@example.com"})

// Evento urgente (prioridade alta)
eventbus.PublishWithPriority(bus, "alert.critical",
AlertData{Level: "CRITICAL"}, 10)
```

Eventos pendentes na fila são ordenados por prioridade; com prioridades
iguais vale a ordem de publicação.

### Cancelando uma inscrição

```go
unsubscribe := eventbus.Subscribe(bus, "order.created", sendEmailHandler)

// Remove o handler quando não for mais necessário
unsubscribe()
```

### Múltiplos Handlers

```go
// Diferentes handlers para o mesmo tipo de evento
eventbus.Subscribe(bus, "order.created", sendEmailHandler)
eventbus.Subscribe(bus, "order.created", updateInventoryHandler)
eventbus.Subscribe(bus, "order.created", notifyWarehouseHandler)

// Todos recebem o mesmo evento tipado!
```

### Monitorando Dead Letter Queue

```go
go func() {
    dlq := bus.GetDLQ()
    for ev := range dlq {
        log.Printf("Evento falhou após %d tentativas: %s - %+v (%v)",
            ev.Attempts, ev.Type, ev.Payload, ev.Err)
        // Aqui você pode: salvar em DB, enviar alerta, etc
    }
}()
```

### Estatísticas

```go
stats := bus.Stats()
fmt.Printf("Handlers: %v\n", stats["handlers_registrados"])
fmt.Printf("Queue: %v/%v\n", stats["queue_size"], config.QueueSize)
```

## Arquitetura

```text
┌─────────────┐
│  Publisher  │
└──────┬──────┘
       │ Publish()
       ▼
┌─────────────────┐
│   Event Queue   │  (buffered channel)
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐ ┌────────┐
│Worker 1│ │Worker N│  (goroutines)
└────┬───┘ └───┬────┘
     │         │
     ▼         ▼
┌──────────────────┐
│     Handlers     │
└────────┬─────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
  Success    Fail → Retry → DLQ
```

## Casos de Uso

- **Microserviços** - Comunicação assíncrona entre serviços
- **Webhooks** - Processar webhooks de forma escalável
- **Background Jobs** - Tarefas assíncronas (emails, notificações, etc)
- **Event Sourcing** - Arquitetura baseada em eventos
- **CQRS** - Separação de comandos e queries

## Performance

- Workers processam eventos em paralelo
- Channels nativos do Go (super eficientes!)
- Zero alocações extras na maioria dos casos
- Backpressure automático quando queue está cheia

## Tratamento de Erros

1. **Handler falha** → Retry com backoff linear configurável, com teto e jitter (só o handler que falhou e os seguintes são reexecutados)
   - Tentativa inicial (imediata)
   - Retry 1: aguarda 1s
   - Retry 2: aguarda 2s
   - Retry N: aguarda Ns
2. **Após todas tentativas** → Chama `OnFailure` (se definido) e envia para DLQ (`DLQSize` negativo desativa a DLQ; `GetDLQ()` retorna `nil`)
3. **DLQ cheia** → Log de erro via `slog`, `OnDLQDrop` é chamado e `Stats()["dlq_dropped"]` é incrementado (evento é perdido)

**Exemplo:** Com `Retries: 3`, cada evento terá **4 tentativas totais** (1 inicial + 3 retries).

## Shutdown Graceful

```go
// Para de aceitar eventos, processa os pendentes e aguarda até 10s;
// se estourar, cancela os handlers em execução
if err := bus.Shutdown(10 * time.Second); err != nil {
    log.Fatal("Timeout no shutdown!")
}
```

## Boas Práticas

✅ Use context nos handlers para timeout/cancelamento  
✅ Mantenha handlers rápidos (< 1s idealmente)  
✅ Configure workers baseado na carga esperada  
✅ Monitore a DLQ para identificar problemas  
✅ Use eventos com prioridade para casos críticos  
✅ Sempre faça shutdown graceful em produção  

## Contribuindo

Pull requests são bem-vindos! Para mudanças grandes, abra uma issue primeiro.

## Licença

Este projeto é distribuído sob a licença MIT. Consulte o arquivo [LICENSE](LICENSE) para obter detalhes.

## Autor

2025, Thiago Zilli Sarmento :heart:
