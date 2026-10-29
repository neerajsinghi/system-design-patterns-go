// Command example runs a support chatbot over the policy documents of a
// fictional retailer, first from "model memory" and then with retrieval-
// augmented generation, and measures how chunking and retrieval mode decide
// whether the right passage reaches the model. Everything is deterministic:
// a toy embedding, BM25 and an extractive fake model, no network.
package main

import (
	"fmt"
	"log"

	ragretrieval "github.com/neerajsinghi/system-design-patterns-go/patterns/ai/rag-retrieval"
)

// modelMemory is what a model trained before 1 March 2026 "knows" about the
// returns policy: the old 14-day window, stated with full confidence.
var modelMemory = map[string]string{
	"return": "You have 14 days from delivery to return an item.",
}

func closedBook(q string) string {
	for _, t := range ragretrieval.Terms(q) {
		if a, ok := modelMemory[t]; ok {
			return a
		}
	}
	return "Our policy allows returns in most cases."
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

const (
	topK   = 3
	budget = 250
)

func main() {
	docs := ragretrieval.Corpus()
	qs := ragretrieval.EvalSet()
	ev := must(ragretrieval.NewEvaluator(docs, qs))
	words := 0
	for _, d := range docs {
		words += ragretrieval.CountTokens(d.Text)
	}
	heading := must(ragretrieval.NewHeadingChunker(120))
	fixed64 := must(ragretrieval.NewFixedChunker(64))
	overlap := must(ragretrieval.NewOverlapChunker(64, 16))
	headIx := must(ragretrieval.NewIndex(ragretrieval.ChunkAll(heading, docs)))

	fmt.Printf("Pebblebrook Home (fictional): %d policy documents, %d words, %d evaluation questions\n\n", len(docs), words, len(qs))

	// 1) Model memory vs RAG.
	q := "How many days do I have to return an item?"
	fmt.Println("1) A customer asks:", q)
	fmt.Printf("   Without retrieval (model memory, trained on the old policy):\n     %q\n", closedBook(q))
	p := must(ragretrieval.NewPipeline(headIx, ragretrieval.WithTopK(topK), ragretrieval.WithTokenBudget(budget)))
	r := must(p.Ask(q))
	fmt.Printf("   With RAG (heading chunks, hybrid, top-%d, %d-token budget):\n", topK, budget)
	for _, c := range r.Prompt.Context {
		fmt.Printf("     [%d] %-20s %s\n", c.N, c.ChunkID, c.Heading)
	}
	fmt.Printf("     answer: %q\n", r.Answer.Text)
	fmt.Printf("     grounded: %v | prompt: %d of %d tokens\n", ragretrieval.Grounded(r.Answer, r.Prompt) == nil, r.Prompt.Tokens, budget)
	fmt.Println("   The documents changed on 1 March 2026; the model's memory did not. RAG answers from")
	fmt.Println("   today's policy and the citation lets anyone check the sentence against passage [1].")
	fmt.Println()

	// 2) Chunking strategy x retrieval mode.
	fmt.Printf("2) Retrieval quality on %d questions: recall@%d / MRR@%d (the answer passage must be in the chunk)\n", len(qs), topK, ragretrieval.MRRDepth)
	fmt.Printf("   %-22s %7s", "chunking", "chunks")
	for _, m := range ragretrieval.Modes {
		fmt.Printf("  %-13s", m)
	}
	fmt.Println()
	for _, c := range []ragretrieval.Chunker{fixed64, overlap, heading} {
		ix := must(ragretrieval.NewIndex(ragretrieval.ChunkAll(c, docs)))
		fmt.Printf("   %-22s %7d", c.Name(), ix.Len())
		for _, m := range ragretrieval.Modes {
			res := must(ev.Evaluate(ix, m, topK))
			fmt.Printf("  %.2f / %.2f  ", res.Recall, res.MRR)
		}
		fmt.Println()
	}
	fmt.Println("   Heading chunks win: each chunk is one whole policy section plus its title, so the")
	fmt.Println("   words a customer uses (\"warranty\", \"parking\") sit next to the answer. Overlap rescues")
	fmt.Println("   some answers that a fixed boundary cut in half. Hybrid is not a clear winner here")
	fmt.Println("   because the toy embedding is itself lexical; with a real embedding model, dense")
	fmt.Println("   catches paraphrases BM25 misses and fusing the two usually beats either alone.")
	fmt.Println()

	// 3) Refuse when nothing relevant was retrieved.
	fmt.Println("3) Out-of-scope questions (refuse if best cosine similarity < 0.25)")
	open := must(ragretrieval.NewPipeline(headIx, ragretrieval.WithMinRelevance(0)))
	for _, oq := range []string{"Do you sell electric cars?", "Do you offer a student discount?"} {
		gated := must(p.Ask(oq))
		ungated := must(open.Ask(oq))
		fmt.Printf("   %q  best similarity %.2f\n", oq, gated.BestDense)
		if gated.Answer.Refused {
			fmt.Printf("     gated:   refused (%s)\n", gated.Reason)
		} else {
			fmt.Printf("     gated:   %q\n", gated.Answer.Text)
		}
		fmt.Printf("     no gate: %q (grounded: %v)\n", ungated.Answer.Text, ragretrieval.Grounded(ungated.Answer, ungated.Prompt) == nil)
	}
	fmt.Println("   Without the gate, \"electric cars\" retrieves the parking section and quotes it: a")
	fmt.Println("   grounded, cited, useless answer. \"Student discount\" shares the word \"discount\" with")
	fmt.Println("   the bulk-order policy, scores above the threshold and is answered wrongly. Grounded")
	fmt.Println("   is not the same as correct; a similarity threshold only catches the obvious misses.")
	fmt.Println()

	// 4) Chunk size.
	fmt.Printf("4) Chunk size (hybrid, top-%d, answers built within a %d-token prompt)\n", topK, budget)
	fmt.Printf("   %-12s %7s %9s %9s %12s %14s\n", "chunking", "chunks", "recall@1", "recall@3", "ctx tokens", "answered right")
	sizes := []ragretrieval.Chunker{}
	for _, n := range []int{8, 16, 32, 64, 128, 256} {
		sizes = append(sizes, must(ragretrieval.NewFixedChunker(n)))
	}
	for _, c := range append(sizes, heading) {
		ix := must(ragretrieval.NewIndex(ragretrieval.ChunkAll(c, docs)))
		r1 := must(ev.Evaluate(ix, ragretrieval.Hybrid, 1))
		r3 := must(ev.Evaluate(ix, ragretrieval.Hybrid, topK))
		acc := must(ev.AnswerAccuracy(ix, ragretrieval.Hybrid, topK, budget))
		fmt.Printf("   %-12s %7d %9.2f %9.2f %12.0f %13.0f%%\n", c.Name(), ix.Len(), r1.Recall, r3.Recall, r3.AvgContextTokens, acc*100)
	}
	fmt.Println("   Too small: an 8-word chunk that holds the answer phrase rarely also holds the words")
	fmt.Println("   the customer used, so nothing ranks it and recall collapses. Too large: a 256-word chunk")
	fmt.Println("   is a whole document, so recall looks great on this small corpus, but usually only one fits")
	fmt.Println("   the budget and the answer must be picked out of a whole document of mixed policy text:")
	fmt.Println("   92% recall@1 becomes 58% right answers. 128-word chunks answer as well as heading chunks")
	fmt.Println("   but send the model about three times the tokens. Recall alone would pick the biggest")
	fmt.Println("   chunks; measure what actually reaches the model, too.")
}
