---
name: software-design-philosophy
description: 'Manage software complexity through deep modules, information hiding, and strategic programming. Use when the user mentions "module design", "API too complex", "shallow class", "complexity budget", or "strategic vs tactical". Covers deep vs shallow modules, red flags for complexity, and comments as design documentation. For code quality, see clean-code. For boundaries, see clean-architecture. Use as guidance when planning and making structural changes or modifying interfaces'
license: MIT
metadata:
  author: wondelai
  version: "1.1.0"
---

# A Philosophy of Software Design Framework

A practical framework for managing the fundamental challenge of software engineering: complexity. Apply these principles when designing modules, reviewing APIs, refactoring code, or advising on architecture decisions. The central thesis is that complexity is the root cause of most software problems, and managing it requires deliberate, strategic thinking at every level of design.

## Core Principle

**The greatest limitation in writing software is our ability to understand the systems we are creating.** Complexity is the enemy. It makes systems hard to understand, hard to modify, and a source of bugs. Every design decision should be evaluated by asking: "Does this increase or decrease the overall complexity of the system?" The goal is not zero complexity -- that is impossible in useful software -- but to minimize unnecessary complexity and concentrate necessary complexity where it can be managed.

## Review Mode

This skill works best as a **repeatable design-review rubric**, not as an open-ended philosophy essay. When using it, always declare the review scope first and keep the score anchored to that exact scope.

Valid review scopes:

- `repo`: overall architecture and package/module boundaries
- `package`: one package or subsystem
- `module`: one file or one tightly related cluster of files
- `diff`: only the current change under review

If the user does not specify a scope, default to the smallest scope that matches the question. Do **not** silently score a whole codebase when the actual task is about one package or one change.

## Scoring Rubric

**Goal: 10/10.** Score using the five dimensions below, each worth `0`, `1`, or `2` points. Always report the sub-scores and total; do not give a single floating, impressionistic number without the breakdown.

| Dimension | 0 | 1 | 2 |
|---------|---|---|---|
| **Interface Simplicity** | Public surface is broad, irregular, or caller-heavy | Mixed: some clean entrypoints, some leaky or over-configured ones | Public surface is compact, regular, and hides complexity |
| **Information Hiding** | Important decisions are duplicated or leak across modules | Some ownership is clear, but shared knowledge still leaks | Design decisions are owned in one place and changes stay local |
| **Change Amplification** | Routine changes require edits in many places | Some shared paths are centralized, but not consistently | Common changes are localized behind deep modules |
| **Unknown Unknowns / Clarity** | It is hard to tell where behavior lives or what must change | Structure is partly discoverable but still surprising | Boundaries, invariants, and likely edit points are obvious |
| **Commented Intent** | Comments are absent, stale, or restate code | Some intent is documented, but unevenly | Comments capture abstraction, invariants, and why where needed |

Scoring rules:

- `9-10`: strong design, only targeted refinements needed
- `7-8`: good design with meaningful complexity debt
- `5-6`: mixed design; several structural issues interfere with changes
- `3-4`: complexity is actively slowing work
- `0-2`: design is unstable or largely tactical

Never change the score unless the same scope is being measured with the same rubric.

## Required Evidence

Every finding and every recommendation must be backed by concrete evidence from the scoped code. Do not make abstract claims like "this feels shallow" without showing why.

For each finding, provide:

1. `Symptom`: change amplification, cognitive load, or unknown unknowns
2. `Evidence`: specific files, interfaces, call sites, repeated logic, or duplicated concepts
3. `Why it matters`: what future change is harder because of this
4. `Suggested fix`: the smallest structural change that would improve the score
5. `Risk of over-correction`: how this fix could make another dimension worse

If you cannot provide evidence, do not score that issue heavily.

## Anti-Goodhart Rules

This skill exists to reduce real complexity, not to maximize a number cosmetically.

Do not reward changes that:

- merely move code without reducing dependencies or caller burden
- merge modules into a larger but less coherent "god module"
- add comments that restate the code but do not capture design intent
- add wrappers, indirection, or configurability without hiding meaningful complexity
- improve one local abstraction while worsening package boundaries or caller complexity elsewhere

A recommendation is only good if it improves one or more rubric dimensions **without materially regressing another**. If there is a tradeoff, state it explicitly.

## Improvement Loop

To create a virtuous feedback loop, use this sequence:

1. `Fix scope`: state exactly what is being reviewed
2. `Score current state`: provide the 5-part rubric with evidence
3. `Choose one bottleneck`: pick the highest-leverage issue, not a laundry list
4. `Predict score movement`: explain which rubric dimensions should improve and which might regress
5. `Make the change`
6. `Re-score the same scope`: use the same rubric and compare before/after

Do not claim improvement unless the after-score references the same scope and evidence standard as the before-score.

## Recommendation Priority

Prioritize suggestions in this order:

1. Remove duplicated knowledge or repeated policy
2. Narrow or simplify interfaces that force callers to know too much
3. Move complexity downward into deeper modules
4. Add comments that capture abstraction or invariants
5. Rename or reshape for clarity

Prefer one high-confidence structural recommendation over many speculative ones.

## Tie-Breakers

When principles conflict, resolve them with these tie-breakers:

- Prefer lower caller complexity over local implementation neatness
- Prefer one deeper module over several shallow cooperating modules
- Prefer explicit ownership of knowledge over temporal decomposition
- Prefer preserving package and architecture boundaries over local consolidation
- Prefer measurable reduction in change amplification over aesthetic consistency

If a recommendation would violate an established architectural invariant, flag it as a non-starter unless the review scope explicitly includes architecture change.

## Output Contract

When using this skill for a review, the output should follow this shape:

1. `Scope`
2. `Score` with sub-scores and total
3. `Findings` ordered by severity or leverage
4. `Best next move` with expected score impact
5. `Re-score guidance` describing how to evaluate the change afterward

Do not present the score first without first stating the scope.

## The Software Design Framework

Six principles for managing complexity and producing systems that are easy to understand and modify:

### 1. Complexity and Its Causes

**Core concept:** Complexity is anything related to the structure of a software system that makes it hard to understand and modify. It manifests through three symptoms: change amplification, cognitive load, and unknown unknowns.

**Why it works:** By identifying the specific symptoms of complexity, developers can diagnose problems precisely rather than relying on vague notions of "messy code." The two fundamental causes -- dependencies and obscurity -- provide clear targets for design improvement.

**Key insights:**
- Change amplification: a simple change requires modifications in many places
- Cognitive load: a developer must hold too much information in mind to make a change
- Unknown unknowns: it is not obvious what needs to be changed, or what information is relevant (the worst symptom)
- Dependencies: code cannot be understood or modified in isolation
- Obscurity: important information is not obvious from the code or documentation
- Complexity is incremental -- it accumulates from hundreds of small decisions, not one big mistake
- The "death by a thousand cuts" nature of complexity means every decision matters

**Code applications:**

| Context | Pattern | Example |
|---------|---------|---------|
| **Change amplification** | Centralize shared knowledge | Extract color constants instead of hardcoding `#ff0000` in 20 files |
| **Cognitive load** | Reduce what developers must know | Use a simple `open(path)` API instead of requiring buffer size, encoding, and lock mode |
| **Unknown unknowns** | Make dependencies explicit | Use type systems and interfaces to surface what a change affects |
| **Dependency management** | Minimize cross-module coupling | Pass data through well-defined interfaces, not shared global state |
| **Obscurity reduction** | Name things precisely | `numBytesReceived` not `n`; `retryDelayMs` not `delay` |

See: [references/complexity-symptoms.md](references/complexity-symptoms.md)

### 2. Deep vs Shallow Modules

**Core concept:** The best modules are deep: they provide powerful functionality behind a simple interface. Shallow modules have complex interfaces relative to the functionality they provide, adding complexity rather than reducing it.

**Why it works:** A module's interface represents the complexity it imposes on the rest of the system. Its implementation represents the functionality it provides. Deep modules give you a high ratio of functionality to interface complexity. The interface is the cost; the implementation is the benefit.

**Key insights:**
- A module's depth = functionality provided / interface complexity imposed
- Deep modules: simple interface, powerful implementation (Unix file I/O, garbage collectors)
- Shallow modules: complex interface, limited implementation (Java I/O wrapper classes)
- "Classitis": the disease of creating too many small, shallow classes
- Each interface adds cognitive load -- more classes does not mean better design
- The best abstractions hide significant complexity behind a few simple concepts
- Small methods are not inherently good; depth matters more than size

**Code applications:**

| Context | Pattern | Example |
|---------|---------|---------|
| **Deep module** | Hide complexity behind simple API | `file.read(path)` hides disk blocks, caching, buffering, encoding |
| **Shallow module** | Avoid thin wrappers that just pass through | A `FileInputStream` wrapped in `BufferedInputStream` wrapped in `ObjectInputStream` |
| **Classitis cure** | Merge related shallow classes | Combine `RequestParser`, `RequestValidator`, `RequestProcessor` into one `RequestHandler` |
| **Method depth** | Methods should do something substantial | A `delete(key)` that handles locking, logging, cache invalidation, and rebalancing |
| **Interface simplicity** | Fewer parameters, fewer methods | `config.get(key)` with sensible defaults, not 15 constructor parameters |

See: [references/deep-modules.md](references/deep-modules.md)

### 3. Information Hiding and Leakage

**Core concept:** Each module should encapsulate knowledge that is not needed by other modules. Information leakage -- when a design decision is reflected in multiple modules -- is one of the most important red flags in software design.

**Why it works:** When information is hidden inside a module, changes to that knowledge require modifying only that module. When information leaks across module boundaries, changes propagate through the system. Information hiding reduces both dependencies and obscurity, the two fundamental causes of complexity.

**Key insights:**
- Information hiding: embed knowledge of a design decision in a single module
- Information leakage: the same knowledge appears in multiple modules (a red flag)
- Temporal decomposition causes leakage: splitting code by when things happen forces shared knowledge across phases
- Back-door leakage through data formats, protocols, or shared assumptions is the subtlest form
- Decorators are frequent sources of leakage -- they expose the decorated interface
- If two modules share knowledge, consider merging them or creating a new module that encapsulates the shared knowledge

**Code applications:**

| Context | Pattern | Example |
|---------|---------|---------|
| **Information hiding** | Encapsulate format details | One module owns the HTTP parsing logic; callers get structured objects |
| **Temporal decomposition** | Organize by knowledge, not time | Combine "read config" and "apply config" into a single config module |
| **Format leakage** | Centralize serialization | One module handles JSON encoding/decoding rather than spreading `json.dumps` everywhere |
| **Protocol leakage** | Abstract protocol details | A `MessageBus.send(event)` hides whether transport is HTTP, gRPC, or queue |
| **Decorator leakage** | Use deep wrappers sparingly | Prefer adding buffering inside the file class over wrapping it externally |

See: [references/information-hiding.md](references/information-hiding.md)

### 4. General-Purpose vs Special-Purpose Modules

**Core concept:** Design modules that are "somewhat general-purpose": the interface should be general enough to support multiple uses without being tied to today's specific requirements, while the implementation handles current needs. Ask: "What is the simplest interface that will cover all my current needs?"

**Why it works:** General-purpose interfaces tend to be simpler because they eliminate special cases. They also future-proof the design since new use cases often fit the existing abstraction. However, over-generalization wastes effort and can itself introduce complexity through unnecessary abstractions.

**Key insights:**
- "Somewhat general-purpose" is the sweet spot between too specific and too generic
- The key question: "What is the simplest interface that will cover all my current needs?"
- General-purpose interfaces are often simpler than special-purpose ones (fewer special cases)
- Push complexity downward: modules at lower levels should handle hard cases so upper levels stay simple
- Configuration parameters often represent failure to determine the right behavior -- each parameter is complexity pushed to the caller
- When in doubt, implement the simpler, more general-purpose approach first

**Code applications:**

| Context | Pattern | Example |
|---------|---------|---------|
| **API generality** | Design for the concept, not one use case | A `text.insert(position, string)` API instead of `text.addBulletPoint()` |
| **Push complexity down** | Handle defaults in the module | A web server that picks reasonable buffer sizes instead of requiring callers to configure them |
| **Reduce configuration** | Determine behavior automatically | Auto-detect file encoding instead of requiring an `encoding` parameter |
| **Avoid over-specialization** | Remove use-case-specific methods | One `store(key, value, options)` instead of `storeUser()`, `storeProduct()`, `storeOrder()` |
| **Somewhat general** | General interface, specific implementation | A `Datastore` interface that currently backs onto PostgreSQL but does not expose SQL concepts |

See: [references/general-vs-special.md](references/general-vs-special.md)

### 5. Comments as Design Documentation

**Core concept:** Comments should describe things that are not obvious from the code. They capture design intent, abstraction rationale, and information that cannot be expressed in code. The claim that "good code is self-documenting" is a myth for anything beyond low-level implementation details.

**Why it works:** Code tells you what the program does, but not why it does it that way, what the design alternatives were, or what assumptions the code makes. Comments capture the designer's mental model -- the abstraction -- which is the most valuable and most perishable information in a system.

**Key insights:**
- Four types: interface comments, data structure member comments, implementation comments, cross-module comments
- Interface comments are the most important: they define the abstraction a module presents
- Write comments first (comment-driven design) to clarify your thinking before writing code
- "Self-documenting code" works only for low-level what; it fails for why, assumptions, and abstractions
- Comments should describe what is not obvious -- if the code makes it clear, don't repeat it
- Maintain comments near the code they describe; update them when the code changes
- If a comment is hard to write, the design may be too complex

**Code applications:**

| Context | Pattern | Example |
|---------|---------|---------|
| **Interface comment** | Describe the abstraction, not the implementation | "Returns the widget closest to the given position, or null if no widgets exist within the threshold distance" |
| **Data structure comment** | Explain invariants and constraints | "List is sorted by priority descending; ties are broken by insertion order" |
| **Implementation comment** | Explain why, not what | "// Use binary search here because the list is always sorted and can contain 100k+ items" |
| **Cross-module comment** | Link related design decisions | "// This timeout must match the retry interval in RetryPolicy.java" |
| **Comment-driven design** | Write the interface comment before the code | Draft the function's contract and behavior first, then implement |

See: [references/comments-as-design.md](references/comments-as-design.md)

### 6. Strategic vs Tactical Programming

**Core concept:** Tactical programming focuses on getting features working quickly, accumulating complexity with each shortcut. Strategic programming invests 10-20% extra effort in good design, treating every change as an opportunity to improve the system's structure.

**Why it works:** Tactical programming appears faster in the short term but steadily degrades the codebase, making every future change harder. Strategic programming produces a codebase that stays easy to modify over time. The small upfront investment compounds -- systems designed strategically are faster to work with after a few months.

**Key insights:**
- Tactical tornado: a developer who produces features fast but leaves wreckage behind; often celebrated short-term but destructive long-term
- Strategic mindset: your primary job is to produce a great design that also happens to work, not working code that happens to have a design
- The 10-20% investment: spend roughly 10-20% of development time on design improvement
- Startups need strategic programming most -- early design shortcuts compound into crippling technical debt as the team grows
- "Move fast and break things" culture (early Facebook) vs design-focused culture (Google) -- Google engineers were more productive on complex systems
- Every code change is an investment opportunity: leave the code a little better than you found it
- Refactoring is not a special event -- it is part of every feature's development

**Code applications:**

| Context | Pattern | Example |
|---------|---------|---------|
| **Tactical trap** | Resist quick-and-dirty fixes | Don't add a boolean parameter to handle "just this one special case" |
| **Strategic investment** | Improve structure during feature work | When adding a feature, refactor the module interface if it has become awkward |
| **Tactical tornado** | Recognize and intervene | A developer who writes 2x the code but creates 3x the maintenance burden |
| **Startup discipline** | Invest in design from day one | Clean module boundaries and good abstractions even under time pressure |
| **Incremental improvement** | Fix one design issue per PR | Each pull request improves at least one abstraction or eliminates one piece of complexity |
| **Design reviews** | Evaluate structure, not just correctness | Code reviews should ask "does this make the system simpler?" not just "does it work?" |

See: [references/strategic-programming.md](references/strategic-programming.md)

## Common Mistakes

| Mistake | Why It Fails | Fix |
|---------|-------------|-----|
| **Creating too many small classes** | Classitis adds interfaces without adding depth; each class boundary is cognitive overhead | Merge related shallow classes into deeper modules with simpler interfaces |
| **Splitting modules by temporal order** | "Read, then process, then write" forces shared knowledge across three modules | Organize around information: group code that shares knowledge into one module |
| **Exposing implementation in interfaces** | Callers depend on internal details; changes propagate everywhere | Design interfaces around abstractions, not implementations; hide format and protocol details |
| **Treating comments as optional** | Design intent, assumptions, and abstractions are lost; new developers guess wrong | Write interface comments first; maintain them as the code evolves |
| **Configuration parameters for everything** | Each parameter pushes a decision to the caller, increasing cognitive load | Determine behavior automatically; provide sensible defaults; minimize required configuration |
| **Quick-and-dirty tactical fixes** | Each shortcut adds a small amount of complexity; over time the system becomes unworkable | Invest 10-20% extra in good design; treat every change as a design opportunity |
| **Pass-through methods** | Methods that just delegate to another method add interface without adding depth | Merge the pass-through into the caller or the callee |
| **Designing for specific use cases** | Special-purpose interfaces accumulate special cases and become bloated | Ask "what is the simplest interface that covers all current needs?" |

## Quick Diagnostic

| Question | If No | Action |
|----------|-------|--------|
| Can you describe what each module does in one sentence? | Modules are doing too much or have unclear purpose | Split into modules with coherent, describable responsibilities |
| Are interfaces simpler than implementations? | Modules are shallow -- they leak complexity outward | Redesign to hide more; merge shallow classes into deeper ones |
| Can you change a module's implementation without affecting callers? | Information is leaking across module boundaries | Identify leaked knowledge and encapsulate it inside one module |
| Do interface comments describe the abstraction, not the code? | Design intent is lost; developers will misuse the module | Write comments that explain what the module promises, not how it works |
| Is design discussion part of code reviews? | Reviews only catch bugs, not complexity growth | Add "does this reduce or increase system complexity?" to review criteria |
| Does each module hide at least one important design decision? | Modules are organized around code, not around information | Reorganize so each module owns a specific piece of knowledge |
| Can a new team member understand module boundaries without reading implementations? | Abstractions are not documented or are too leaky | Improve interface comments and simplify interfaces until they are self-evident |
| Are you spending 10-20% of time on design improvement? | Technical debt is accumulating with every feature | Adopt a strategic mindset; include design improvement in every PR |

## Repeatable Review Checklist

Use this checklist before finalizing a score or recommendation:

- Did I state the exact review scope?
- Did I use the 5-dimension rubric instead of an ad hoc score?
- Did each finding include concrete evidence?
- Did I identify the complexity symptom involved?
- Did I explain what future change becomes easier or harder?
- Did I check for over-correction risk?
- Am I recommending the highest-leverage change, not just the easiest cosmetic one?
- If I am comparing scores, am I scoring the same scope before and after?

## Reference Files

- [complexity-symptoms.md](references/complexity-symptoms.md): Three symptoms of complexity, two causes, measuring complexity, the incremental nature of complexity
- [deep-modules.md](references/deep-modules.md): Deep vs shallow modules, interface-to-functionality ratio, classitis, designing for depth
- [information-hiding.md](references/information-hiding.md): Information hiding principle, information leakage red flags, temporal decomposition, decorator pitfalls
- [general-vs-special.md](references/general-vs-special.md): Somewhat general-purpose approach, pushing complexity down, configuration parameter antipattern
- [comments-as-design.md](references/comments-as-design.md): Four comment types, comment-driven design, self-documenting code myth, maintaining comments
- [strategic-programming.md](references/strategic-programming.md): Strategic vs tactical mindset, tactical tornado, investment approach, startup considerations

## Further Reading

This skill is based on John Ousterhout's practical guide to software design. For the complete methodology with detailed examples:

- [*"A Philosophy of Software Design"*](https://www.amazon.com/Philosophy-Software-Design-2nd/dp/173210221X?tag=wondelai00-20) by John Ousterhout (2nd edition)

## About the Author

**John Ousterhout** is the Bosack Lerner Professor of Computer Science at Stanford University. He is the creator of the Tcl scripting language and the Tk toolkit, and co-founded several companies including Electric Cloud and Clustrix. Ousterhout has received numerous awards, including the ACM Software System Award, the UC Berkeley Distinguished Teaching Award, and the USENIX Lifetime Achievement Award. He developed *A Philosophy of Software Design* from his CS 190 course at Stanford, where students work on multi-phase software design projects and learn to recognize and reduce complexity. The book distills decades of experience in building systems software and teaching software design into a concise set of principles that apply across languages, paradigms, and system scales. Now in its second edition, the book has become a widely recommended resource for software engineers seeking to improve their design skills beyond correctness and into clarity.
