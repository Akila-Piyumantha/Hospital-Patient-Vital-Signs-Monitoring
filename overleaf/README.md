# Overleaf project files

Everything needed for the LaTeX version of the report, kept separate from the exported
`Report.pdf`/`Report.docx` at the repo root so the source and the figures can be uploaded to
Overleaf directly.

## What's here

```
overleaf/
├── main.tex              the whole report, ready to compile as-is
└── figures/
    ├── architecture-diagram.png   the drawn architecture diagram (Section 3)
    ├── fig-pipeline-health.png    pipeline health dashboard (Section 8)
    ├── fig-ward-live.png          ward live monitoring dashboard (Section 8)
    ├── fig-daily-report.png       sample daily consolidated risk report (Section 8)
    └── fig-api-sample.png         sample /api/ward/summary response (Section 8)
```

## Uploading to Overleaf

1. Create a new **Blank Project** on Overleaf.
2. Delete the default `main.tex` it creates, then upload this folder's `main.tex` in its place
   (drag-and-drop, or the upload button in the file list).
3. Create a folder named `figures` in the Overleaf project (the "New Folder" button), and upload
   all five PNGs from `overleaf/figures/` into it, keeping the same filenames.
4. Compiler: **pdfLaTeX** (Overleaf's default). No extra packages need installing — everything
   used (`geometry`, `graphicx`, `booktabs`, `longtable`, `hyperref`, etc.) ships with Overleaf's
   standard TeX Live image.
5. Click **Recompile**.

## Checked before hand-off

The source was validated (brace balance, matched `\begin`/`\end` environments, no unescaped
underscores) but not compiled locally, since this machine has no LaTeX toolchain installed —
Overleaf's own compiler is the first real compile. If it reports an error, it's almost certainly a
single line; paste the error back and it's a quick fix.

## Editing

The two team members whose names are placeholders (`Member B`, and if needed a full-name
correction for Member A/C) are in the "Individual contributions" table near the end of `main.tex` —
search for `to be confirmed`.
