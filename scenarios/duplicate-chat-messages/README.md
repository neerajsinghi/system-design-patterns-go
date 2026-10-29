# Scenario: The Chat App Shows the Same Message Twice

## The incident

A messaging app starts getting screenshots from annoyed users. The same message shows up twice in a conversation, sometimes three times or more, and it's always from someone who was on a train or in a lift when they sent it. Other users complain about the opposite problem. They sent a message, the app showed it as failed or never ticked it, and the other person never got it. A few say they saw a message marked as failed that the other person had in fact received. Every server metric looks healthy. Nothing errors. The server stored and delivered exactly what it was sent.

## Symptoms

- Duplicate bubbles cluster around senders and recipients with weak mobile signal, and a duplicate usually lands a second or two after the original.
- Some messages appear three or more times: once per resend from the sender, and again for each repeated push to the recipient.
- Messages sent while a phone is in a dead zone (a tunnel, a lift) vanish if the dead zone lasts more than a couple of seconds.
- Some messages show up in the wrong place in the conversation, below messages that were sent after them.

## Root cause

1. **No stable message ID.** The app sends the text and lets the server create the message. When the server's ack is lost on the way back, the app can't tell "the server never got it" from "the server got it and the reply was lost", so it sends again. The server sees a new request, gives it a new ID, and stores a second message. Nothing downstream can tell the two apart.
2. **The push leg has the same problem.** The server pushes the message to the recipient's phone and waits for an ack. When that ack is lost, it pushes again, and the phone appends whatever it receives.
3. **Retries that give up too soon.** Both sides retry three times, a second apart, then stop. That is enough to turn a lost ack into a duplicate, and not enough to get through a ten-second tunnel.
4. **Arrival order is not send order.** The conversation shows messages in the order they reached the phone, so a message that needed a retry lands below a newer one.

## The fix

| Change | Pattern | Effect |
| --- | --- | --- |
| The sending phone gives each message a Snowflake ID once, before the first attempt, and resends with the same ID | [Snowflake ID](../../patterns/data/snowflake-id), the producer side of an [idempotent consumer](../../patterns/distributed/idempotent-consumer) | A retransmission is recognisably the same message |
| The server checks the ID against an inbox in the same transaction that stores the message, before fan-out, and acks a duplicate without storing it again | [Idempotent consumer](../../patterns/distributed/idempotent-consumer) (inbox with a retention window) | Each message is stored and pushed once, however many times it was sent |
| The recipient's phone keeps its own inbox of IDs it has shown | [Idempotent consumer](../../patterns/distributed/idempotent-consumer) (device-side) | A repeated push is dropped instead of drawn |
| Both legs retry with capped exponential backoff and jitter, up to 10 attempts | [Retry with backoff](../../patterns/reliability/retry-backoff) | Retries spread out past a dead zone instead of all landing inside it |
| The conversation is sorted by message ID | [Snowflake ID](../../patterns/data/snowflake-id) (time-ordered) | A late retransmission appears where it was sent, not where it arrived |

## Run it

```sh
go run ./scenarios/duplicate-chat-messages/broken
go run ./scenarios/duplicate-chat-messages/fixed
go test ./scenarios/duplicate-chat-messages/
```

| | Broken | Fixed |
| --- | --- | --- |
| Bubbles in Bob's conversation, for 300 messages sent | 348 | 300 |
| Duplicate bubbles | 91 (73 messages shown more than once, up to 7×) | 0 |
| Lost messages | 43 (18 never reached the server, 25 lost on the push) | 0 |
| Messages shown out of send order | 16 | 0 |
| Sender gave up without an ack | 21 | 0 |
| Client retransmits | 108 | 148 |
| Copies the server stored and fanned out | 327 | 300 |
| Retransmits caught by the server's dedup store | 0 | 55 |
| Server push retries | 138 | 173 |
| Repeat pushes dropped by Bob's phone | 0 | 54 |
| Peak dedup store size | none | 130 IDs (2-minute window) |

The fixed version makes more attempts than the broken one, not fewer. Backoff is not there to save attempts here. It makes the same small number of attempts per second last long enough to outlive a dead zone, where the broken client spends all three of its attempts inside one. Of the broken run's 21 "gave up" messages, 3 had in fact reached the server and only the acks were lost: those are the messages the sender sees as failed but the recipient has. The 7× copy comes from both legs multiplying: three attempts reaching the server make three stored copies, and each of those can be pushed up to three times.

The tests check that the broken run shows duplicates (one message at least 3 times), loses messages on both legs, and never deduplicates anything. They check that the fixed run shows every message exactly once, in send order, with no sender giving up, that both dedup layers caught real duplicates, and that the server stored exactly one copy per message. One test pins the cost of the retry change: the fixed client retransmits more, because it keeps going where the broken one gives up. Two tests pin the cost of the dedup store. The server's dedup store only protects for its retention window. A retransmission exactly 2 minutes after the first arrival is caught, and one 1 millisecond later slips through and is stored and fanned out again. So the window has to outlast the longest a client can keep retransmitting (1m28.5s with this policy), and the store holds roughly window × message rate IDs. Another test pins the clock-skew caveat of ordering by Snowflake ID. A reply from a phone whose clock is 3 seconds slow gets an ID lower than the question it answers, and sorts above it. A last test checks both runs are deterministic.

**How the simulation is built.** Alice sends 300 messages to Bob, 0.4-1.6 seconds apart, from a seeded generator. Each phone's link has its own 4 dead zones of 4-15 seconds at seeded random times. Outside them, an attempt is lost on the way out 5% of the time, and its ack is lost 15% of the time. An attempt made just before a dead zone starts can reach the other side and lose its ack in the dead zone. The fate of an attempt depends only on when it is made and on (leg, message, stored copy, attempt number), so both designs see identical faults for every attempt they both make. A sender waits a 1-second ack timeout before retrying. Broken retries immediately after the timeout, 3 attempts in all. Fixed uses `retrybackoff.Policy` (10 attempts, 500ms base, 16s cap, equal jitter), whose `Sleep` advances the attempt's simulated clock. The run goes in three phases on one simulated clock. Alice's phone sends every message and records which attempts reach the server. The server then handles those arrivals in time order: Broken stores each one under a new server ID, Fixed runs it through an `idempotentconsumer.Store` inbox with a 2-minute retention. Each stored copy is pushed to Bob with the same sender logic, and Bob's phone handles the pushes in time order (Fixed through a second inbox with no retention). Alice's phone uses a `snowflakeid.Generator` on node 1, reading the simulated clock.

**Simplifications.** Alice is the only sender and Bob has one device, so "fan-out" is a single push. In a group chat or with several devices the server-side check matters more, because one duplicate becomes one extra push per member device. The ack decision is made by the network model, not by the server, so the server always acks what it receives, including duplicates. That re-ack matters in a real system, because a server that drops a duplicate silently leaves the sender retrying forever. Real clients keep an outbox on disk and retry until they get an ack, with no attempt limit. The 10-attempt cap here only keeps the worst-case retransmit span below the dedup window, and the boundary test shows what happens when that stops being true. Phones in production don't usually get a Snowflake node ID: there are too many of them for 10 bits. Systems typically dedup on (sender user ID, client-generated ID or per-device sequence number) and let the server assign the ordering key. The in-process stores stand in for a database table with a unique index on the message ID.

## Interview follow-ups

- *Why not just have the server dedup on the message text and sender?* People send "ok" twice on purpose. Content isn't identity. Only an ID created before the first attempt can tell a retransmission from a second message.
- *Who should generate the ID, the client or the server?* The client: only it knows that two requests are the same send. The server can still assign its own ID for storage and ordering, keyed by the client's ID, as long as the client's ID is what dedup checks.
- *How long should the server remember IDs?* Longer than the longest time a client can keep resending: its outbox retry span, plus how long it might be offline with the message queued. That can be days. A common answer is a short in-memory window for the hot path, backed by a unique constraint on (sender, client ID) in the message table, so the database catches what the window misses.
- *Isn't the phone's dedup enough on its own?* It hides duplicates from one device, but the server has already stored two messages, every other device and group member still gets both, and search, unread counts and backups are all wrong. Dedup at the server before fan-out, and at the device as a second line.
- *Is sorting by Snowflake ID safe?* With one sender it gives send order. Across senders it trusts every phone's clock, and phone clocks drift or are set wrong by hand, so a reply can sort above the question. Many systems order by a server-assigned sequence per conversation, or by server receive time, and keep the client ID only for dedup. Lamport or vector clocks ([Lamport and vector clocks](../../patterns/distributed/lamport-vector-clocks)) capture "this reply was written after that message" without trusting wall clocks.
- *The server acked, but the message never reached the recipient. Whose job is that?* The server's. Once it acks, it owns delivery, so the push queue has to be durable and keep retrying until the device acks, even across reconnects. "Delivered" ticks come from the recipient's ack, not the server's.
- *Why not exactly-once delivery?* Over a lossy network, a sender can't know whether a message whose ack was lost got through, so it either resends (at-least-once) or doesn't (at-most-once). Resending plus dedup at the receiver gives exactly-once effect, which is what the user sees.

## Further reading

- Arvind Kumar, [Duplicate Messages in WhatsApp](https://codefarm0.medium.com/duplicate-messages-in-whatsapp-system-design-interview-deep-dive-on-at-least-once-delivery-and-cecdc091fc57), the interview scenario this walkthrough is based on
- Khyati Jha, [High Level System Design Day-18: WebSockets (Part 6)](https://medium.com/high-level-system-desgin-interview-prep/high-level-system-design-day-178-websockets-part-6-2be2650b7b2f), part of a series on WebSockets, the persistent connections chat apps push messages and acks over
