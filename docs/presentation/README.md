# Sprint Presentation Guidelines & Merging Instructions

This directory manages all presentation slides and deliverables for Sprint Review meetings. To ensure each member's slides are **modular, easy to design using AI**, and **seamlessly consolidated** into a single final presentation deck without styling conflicts, all team members must follow the guidelines outlined below.

---

## 1. Directory Structure

- `sprint03/`: Presentation decks for Sprint 03.
  - `rookie12/`, `rookie14/`, `rookie15/`, `rookie16/`, `rookie17/`, `rookie20/`: Individual workspace folders. Each rookie edits their own `presentation.html` here.
  - `sprint03-final/`: Folder containing the consolidated final slide deck.
- `sprint04/`: Presentation decks for Sprint 04.

---

## 2. Slide Design Specifications (For Rookies)

To ensure your slides look modern, are easy to read, and **do not conflict with other members' CSS** when merged, follow these four golden rules:

### 🌟 4 Golden Rules:
1. **Single File:** Your entire presentation must be written in a **single file** named `presentation.html` (including HTML structure and all styling).
2. **CSS Isolation:** 
   - Wrap all of your slides inside a single parent container with your unique Rookie ID. Example: `<div id="rookie12-slides">`.
   - All CSS rules inside your `<style>` block **must** be prefixed with this ID to prevent leakage to other members' slides.
     * *Incorrect:* `h1 { color: blue; }` (this will change the color of all `h1` headings in the consolidated deck).
     * *Correct:* `#rookie12-slides h1 { color: blue; }` (limits styling only to your slides).
3. **Clear Slide Separation:** Wrap each individual slide page inside a `<section class="slide-item">` tag.
4. **No Custom JS Navigation:** Only write static HTML and CSS (flexbox, grid, animations). Slide transitions and navigation controls will be handled globally by the engine when merged.

---

## 3. AI Prompting Guide for Rookies

When using ChatGPT, Claude, or Gemini to generate your slides, copy and adapt the prompt template below. This prompt is designed to produce code that is 100% compliant with our repository structure.

### 📋 Prompt Template (Copy & Paste):

> **Context:** I am a Data Engineering Rookie working on the Insurance Analytics project. I need to design an HTML/CSS slide deck for my Sprint Review presentation in **[Insert Sprint Name, e.g., Sprint 03]**.
>
> **My Presentation Topic & Content:**
> [Describe your slide outline briefly, e.g.,
> - Slide 1: Introduction, role, and Sprint 03 goals.
> - Slide 2: Ingestion pipeline design for the Bronze Layer.
> - Slide 3: SQL transformations and table architecture.
> - Slide 4: Challenges faced and resolution strategies.]
>
> **Technical Requirements for HTML/CSS Generation:**
> 1. Generate a **single HTML file**. All styles must be written inside an internal `<style>` block in the `<head>` section. No external CSS link stylesheet.
> 2. Wrap the entire body content inside a single parent div tag with the unique ID: `<div id="rookie[Insert your number, e.g., rookie12]-slides">`.
> 3. For CSS isolation, all CSS rules must be prefixed with this ID. For example: `#rookie12-slides .slide-item`, `#rookie12-slides h1`, `#rookie12-slides p`, `#rookie12-slides pre`. Never style generic selectors like `body`, `h1`, or `p` globally without this prefix.
> 4. Structure each individual slide page as a `<section class="slide-item">` container placed directly inside the parent div.
> 5. Design a modern, premium UI/UX (Use clean Google Fonts like Inter or Outfit, a matching dark mode palette or subtle gradient backgrounds, and center elements using Flexbox or CSS Grid). Code blocks (SQL/Python) must be styled beautifully in `<pre><code>` tags.
> 6. Do not include JavaScript for slide switching/navigation. Focus on the static visual layout only.

---

## 4. Skeleton Template

You can provide this template to the AI model to show it the exact structural layout required:

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Rookie 12 - Sprint Presentation</title>
    <!-- Premium Google Font -->
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap" rel="stylesheet">
    
    <style>
        /* Scoped styles under the Rookie unique ID prefix */
        #rookie12-slides {
            font-family: 'Outfit', sans-serif;
            color: #f3f4f6;
            background: #111827;
            padding: 20px;
        }

        #rookie12-slides .slide-item {
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            border-bottom: 2px dashed #374151; /* Visual separator for dev/preview */
            padding: 40px;
            box-sizing: border-box;
        }

        #rookie12-slides h1 {
            font-size: 3rem;
            color: #60a5fa; /* Primary accent color */
            margin-bottom: 20px;
        }

        #rookie12-slides p {
            font-size: 1.25rem;
            max-width: 800px;
            text-align: center;
            line-height: 1.6;
            color: #d1d5db;
        }

        #rookie12-slides pre {
            background: #1f2937;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #374151;
            text-align: left;
        }
    </style>
</head>
<body>

    <!-- Scoped container ID -->
    <div id="rookie12-slides">

        <!-- Slide 1 -->
        <section class="slide-item">
            <h1>Gold Layer Pipeline Optimization</h1>
            <p>Sprint 03 Progress Report - Rookie 12</p>
        </section>

        <!-- Slide 2 -->
        <section class="slide-item">
            <h1>Key Achievements</h1>
            <p>Designed and deployed 5 dimension tables and 2 fact tables for insurance analytics.</p>
            <pre><code>SELECT * FROM gold.dim_customer LIMIT 5;</code></pre>
        </section>

    </div>

</body>
</html>
```

---

## 5. Merger Instructions for the Final Slide Deck

When combining individual slides into [final-slide.html](sprint03/sprint03-final/final-slide.html), the consolidation script or AI should execute these steps:

1. **Extract CSS:** Parse all styles from each Rookie's `<style>` block and append them into the main `<style>` section of the master template.
2. **Extract HTML:** Copy the complete `<div id="rookieXX-slides">` block of each member and insert them sequentially into the `<body>` of the master template.
3. **Embed Slide Engine:** Implement a simple slider engine (such as a lightweight JS router or the Reveal.js library) in the master template to toggle visibility of `.slide-item` containers on arrow key triggers.
