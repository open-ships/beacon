---
name: Beacon
description: Ship-board NMEA 2000 routing control for operators, developers, and maritime AI systems.
colors:
  accent: "#2337ff"
  accent-dark: "#000d8a"
  link: "#40577f"
  metric: "#2f3d55"
  ink: "#0f1219"
  text: "#222939"
  muted-text: "#555555"
  blue-gray: "#60739f"
  surface: "#ffffff"
  surface-muted: "#f6f8fb"
  surface-strong: "#eef2f7"
  border: "#d9e0eb"
  border-strong: "#c8d2e1"
  success: "#0f7a4f"
  success-bg: "#e8f7ef"
  warning: "#a65f00"
  warning-bg: "#fff4df"
  error: "#b42318"
  error-bg: "#fff0ef"
typography:
  headline:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif"
    fontSize: "2rem"
    fontWeight: 850
    lineHeight: 1.12
    letterSpacing: "0"
  title:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif"
    fontSize: "1.12rem"
    fontWeight: 800
    lineHeight: 1.2
    letterSpacing: "0"
  body:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif"
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: 1.5
    letterSpacing: "0"
  label:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif"
    fontSize: "0.78rem"
    fontWeight: 800
    lineHeight: 1.2
    letterSpacing: "0"
  mono:
    fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, monospace"
    fontSize: "0.88em"
    fontWeight: 400
    lineHeight: 1.4
rounded:
  xs: "4px"
  sm: "6px"
  md: "8px"
  pill: "999px"
spacing:
  xs: "4px"
  sm: "8px"
  md: "12px"
  lg: "16px"
  xl: "18px"
  xxl: "24px"
components:
  button-default:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.text}"
    rounded: "{rounded.sm}"
    padding: "0.55rem 0.86rem"
    height: "2.35rem"
  button-primary:
    backgroundColor: "{colors.accent}"
    textColor: "{colors.surface}"
    rounded: "{rounded.sm}"
    padding: "0.55rem 0.86rem"
    height: "2.35rem"
  button-danger:
    backgroundColor: "{colors.error}"
    textColor: "{colors.surface}"
    rounded: "{rounded.sm}"
    padding: "0.55rem 0.86rem"
    height: "2.35rem"
  input:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.text}"
    rounded: "{rounded.sm}"
    padding: "0.55rem 0.7rem"
    height: "2.45rem"
  badge:
    backgroundColor: "{colors.surface-muted}"
    textColor: "{colors.muted-text}"
    rounded: "{rounded.pill}"
    padding: "0.22rem 0.5rem"
    height: "1.35rem"
  card:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.text}"
    rounded: "{rounded.md}"
    padding: "18px"
---

# Design System: Beacon

## 1. Overview

**Creative North Star: "Quiet Bridge Console"**

Beacon extends the Open Ships visual system into an operational product surface. It uses the same white field, blue accent, gray text system, Inter/system sans typography, and compact technical tone as openships.ai, then tightens the density for repeated ship-board configuration work.

The design should feel like quiet industrial control: precise, stable, and readable under pressure. The interface is allowed to be dense because the domain is dense, but hierarchy must keep the source -> connector -> sink routing model obvious for operators, developers, and maritime AI systems.

It explicitly rejects generic SaaS dashboard styling, ornamental nautical theming, cloud-first marketing posture, heavy hero composition, and any treatment that makes routing state harder to scan.

**Key Characteristics:**
- White surfaces with cool gray tonal layers and a restrained blue action accent.
- Compact navigation, tables, forms, and badges built for scanning rather than promotion.
- High-confidence state language for up, degraded, error, disabled, restarting, queued, and replay behavior.
- Existing Open Ships vocabulary carried through without decorative maritime motifs.

## 2. Colors

Beacon uses the Open Ships restrained product palette: white surfaces, cool gray structure, deep ink text, one saturated blue for primary action, and semantic state colors only when state matters.

### Primary
- **Open Ships Action Blue** (`accent`): The only saturated brand/action color. Use for primary actions, active focus borders, links, and selected or current affordances.
- **Deep Action Blue** (`accent-dark`): Hover and pressed state for primary actions and links. It should appear only as a response to interaction.

### Neutral
- **Bridge Ink** (`ink`): Highest-emphasis headings, active navigation, and dark connector nodes.
- **Instrument Text** (`text`): Default body and control text.
- **Operational Gray** (`muted-text`): Secondary labels, table headings, eyebrows, and navigation at rest.
- **Blue Gray Signal** (`blue-gray`): Shadow and muted structure color; do not use it as low-contrast body copy.
- **Console Surface** (`surface`): Main page and card surface.
- **Muted Surface** (`surface-muted`): Table heads, subtle panels, hover backgrounds, and form-card backgrounds.
- **Strong Surface** (`surface-strong`): Inline code backgrounds and stronger neutral fills.
- **Hairline Border** (`border`) and **Strong Border** (`border-strong`): One-pixel structure for panels, tables, controls, and dividers.

### Tertiary
- **Up Green** (`success`) and **Up Green Wash** (`success-bg`): Healthy component state and used endpoint indicators.
- **Caution Amber** (`warning`) and **Caution Amber Wash** (`warning-bg`): Degraded, restarting, unused, or caution states.
- **Fault Red** (`error`) and **Fault Red Wash** (`error-bg`): Validation errors, failed components, and destructive actions.

### Named Rules
**The One Accent Rule.** The saturated blue accent is for primary action, focus, and selection, not routine data or decoration. Entity names and live metrics use ink; ordinary links use the muted blue-gray link color. If a screen looks blue at a glance, the accent has been overused.

**The State Earns Color Rule.** Green, amber, and red appear only when they communicate live state, validation, or destructive consequence.

## 3. Typography

**Display Font:** Inter/system sans stack.
**Body Font:** Inter/system sans stack.
**Label/Mono Font:** UI monospace stack for IDs, code, paths, and protocol-ish values.

**Character:** One sans family carries the whole product. Weight, case, and density create hierarchy; there is no separate display voice for routine UI.

### Hierarchy
- **Headline** (850, `2rem`, `1.12`): Dashboard and empty-state headlines. Keep short and direct.
- **Title** (800, `1.12rem`, `1.2`): Card titles, form titles, and compact panel headers.
- **Body** (400, `1rem`, `1.5`): Default UI copy and form text. Long manual prose may loosen to `1.75` and cap around `78ch`.
- **Label** (800, `0.78rem`, uppercase where already established, `letter-spacing: 0`): Table headers, DAG labels, stat labels, and badges.
- **Code** (mono, `0.88em`): IDs, paths, protocol values, headers, and message examples.

### Named Rules
**The Product Sans Rule.** Do not introduce display fonts, serif accents, or decorative type treatments inside the operator UI.

**The Zero Tracking Rule.** Beacon uses uppercase labels with `letter-spacing: 0`; do not add wide-tracked eyebrows or marketing kickers.

## 4. Elevation

Beacon uses a hybrid of one-pixel borders, tonal layering, and a single soft ambient shadow. Most structure should come from borders and surface changes; shadows mark major panels and tables, not every repeated item.

### Shadow Vocabulary
- **Panel Shadow** (`0 2px 6px rgba(96, 115, 159, 0.16), 0 12px 32px rgba(96, 115, 159, 0.12)`): Use on major dashboard boards, cards, metadata sections, and full tables that need to lift from the page background.

### Named Rules
**The Border-First Rule.** Reach for a one-pixel border before a shadow. Shadows are for major containers, not routine controls.

**The No Glass Rule.** Backdrop blur belongs only to the fixed header treatment already in the system. Do not add glass cards or blurred decorative panels.

## 5. Components

### Buttons
- **Shape:** Compact rounded rectangle (6px radius), minimum height `2.35rem`, bold label weight `750`.
- **Primary:** Blue fill with white text. Use for the main create/save action on a surface.
- **Default:** White fill, strong border, dark text. Use for secondary commands and cancel-like actions.
- **Danger:** Red fill with white text. Use only for destructive actions.
- **Hover / Focus:** Hover darkens primary to `accent-dark`; default buttons move to muted surface. Focus must remain visible through border and outline treatment, matching the input focus vocabulary.

### Badges
- **Style:** Pill-shaped, uppercase, compact, bold labels with `1px` borders and low-chroma state washes.
- **State:** Success, warning, error, and ghost badges identify live or enabled state. Do not use badges as decoration.

### Cards / Containers
- **Corner Style:** 8px radius.
- **Background:** White for primary containers, muted surface for form panels when they need separation.
- **Shadow Strategy:** Major panels may use Panel Shadow; nested or repeated items should use border-only treatment.
- **Border:** One-pixel gray border is the default structural device.
- **Internal Padding:** 18px for cards and sections; smaller utility clusters use the spacing scale.

### Inputs / Fields
- **Style:** White background, 1px strong border, 6px radius, `2.45rem` minimum height.
- **Focus:** Blue border plus translucent blue outline (`3px solid rgba(35, 55, 255, 0.12)`).
- **Error / Disabled:** Errors use the red alert vocabulary; disabled fields should stay readable and clearly non-editable.
- **Creation:** Add source, sink, and connector flows open in modal dialogs. Their eight-character hexadecimal IDs are generated and hidden; Enabled is the first field and starts checked.

### Navigation
- **Style:** Compact top header with Open Ships brand context and Beacon product label. Resting links are gray and bold; active links invert to dark fill and white text.
- **Behavior:** Keep navigation stable and small. Do not turn product navigation into a marketing hero or oversized app chrome.

### DAG Board
- **Style:** Centered five-column source -> connector -> sink graph with compact cards, generous routing gutters, centered lane headings, and curved, round-dotted, arrowless links. The panel is bounded at `80rem` rather than filling the application shell.
- **Topology:** Enabled sources and sinks without a configured connector move to the separated unused-endpoints row beneath their matching lane. Disabled endpoints stay in the main graph, and a disabled connector still counts as an existing connection.
- **State:** Component status appears through badges and border tone, never a status-tinted card background. Unused sinks use a muted neutral surface while retaining their real health badge.
- **Motion:** Graph edges make one synchronized brightness flash per second without changing geometry. `prefers-reduced-motion` renders the dotted lines steadily.
- **Caution:** Do not extend the existing colored side-stripe treatment. New graph/state work should use full borders, badges, or icons instead.

### Tables
- **Style:** Full-width bordered tables with muted header rows, compact uppercase headings, and generous enough cell padding for scanning.
- **Behavior:** Tables may overflow horizontally on small screens; preserve column meaning rather than squeezing text until it wraps badly.

## 6. Do's and Don'ts

### Do:
- **Do** preserve openships.ai's white surface, compact navigation, gray text system, blue accent, and practical documentation tone.
- **Do** keep source -> connector -> sink visible whenever the task is about routing or setup.
- **Do** use blue only for links, primary actions, focus, and active selection.
- **Do** make degraded, disabled, restarting, queued, and error states explicit in text, not color alone.
- **Do** favor dense, stable controls and tables for repeated operator workflows.

### Don't:
- **Don't** drift away from the openships.ai visual system.
- **Don't** use generic SaaS dashboard styling, ornamental nautical styling, cloud-first positioning, or heavy marketing composition.
- **Don't** add wide-tracked section eyebrows, gradient text, glass cards, decorative motion, or full-saturation inactive states.
- **Don't** add new `border-left` or `border-right` colored stripes greater than 1px; replace that pattern with full borders, badges, icons, or background tints when touching graph nodes.
- **Don't** introduce display fonts, decorative icons, or invented controls where a standard product affordance already works.
