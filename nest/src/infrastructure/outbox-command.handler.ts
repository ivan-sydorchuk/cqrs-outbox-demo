import { ICommand } from '@nestjs/cqrs';
import { AggregateRoot } from '@nestjs/cqrs';
import { Prisma } from '@prisma/client';
import { DomainEvent } from '../domain/domain-event';
import { PrismaService } from './prisma.service';

interface HandleResult<TResult> {
  result: TResult;
  /** Every aggregate this command touched — the base class pulls their uncommitted events off them. */
  aggregates: AggregateRoot[];
}

/**
 * This is where the "magic" from the practice task lives on the NestJS side —
 * the direct counterpart of AppDbContext.SaveChangesAsync on the .NET side.
 *
 * A concrete handler only implements `handle()`: load/create an aggregate,
 * call a domain method, write its own row via the given transactional Prisma
 * client, and hand back which aggregate(s) it touched. `execute()` — called
 * by NestJS's CommandBus for every command — wraps that in a single Prisma
 * interactive transaction, and after `handle()` returns, pulls
 * `getUncommittedEvents()` off each aggregate and writes them to the outbox
 * table inside that SAME transaction before it commits. The handler never
 * imports PrismaService's non-transactional client, never references the
 * outbox table, and never calls commit() itself.
 */
export abstract class OutboxCommandHandler<TCommand extends ICommand, TResult = void> {
  protected constructor(private readonly prisma: PrismaService) {}

  async execute(command: TCommand): Promise<TResult> {
    return this.prisma.$transaction(async (tx) => {
      const { result, aggregates } = await this.handle(command, tx);
      await this.persistOutbox(tx, aggregates);
      return result;
    });
  }

  protected abstract handle(
    command: TCommand,
    tx: Prisma.TransactionClient,
  ): Promise<HandleResult<TResult>>;

  private async persistOutbox(tx: Prisma.TransactionClient, aggregates: AggregateRoot[]): Promise<void> {
    const events = aggregates.flatMap((aggregate) => aggregate.getUncommittedEvents() as DomainEvent[]);

    // outbox.transaction_id is Unsupported("xid8") in the Prisma schema
    // (Block 10) — Prisma disables batch write methods like createMany() on
    // any model that has an Unsupported column, so this goes through
    // $executeRaw instead. transaction_id itself is never set here: the
    // column's DB-level DEFAULT pg_current_xact_id() (see
    // sql/block10-worker-extension.sql) fills it in, evaluated in this same
    // transaction.
    for (const event of events) {
      await tx.$executeRaw`
        INSERT INTO outbox (aggregate_id, event_type, payload, occurred_at)
        VALUES (${event.aggregateId}::uuid, ${event.constructor.name}, ${JSON.stringify(event)}::jsonb, ${event.occurredAt})
      `;
    }

    // Only reached if the transaction is still going to commit, so a thrown
    // error upstream leaves events sitting on the aggregate uncommitted.
    aggregates.forEach((aggregate) => aggregate.commit());
  }
}
