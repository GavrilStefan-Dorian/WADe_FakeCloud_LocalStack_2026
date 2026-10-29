# Final presentation

- [FakeCloud-vs-LocalStack.pdf](FakeCloud-vs-LocalStack.pdf): the final 29-slide PDF, including selected references on the closing page.
- [FakeCloud-vs-LocalStack.pptx](FakeCloud-vs-LocalStack.pptx): editable slides with Romanian presenter notes.
- [source/](source/): canonical content, source bibliography and matching PDF/PowerPoint renderer.

The final binaries are preserved from the delivered presentation. Development drafts, rejected designs and rehearsal documents were removed after the presentation; the canonical source retains the speaking notes and bibliography.

The public repository includes the exports and this README. The `source/`, `assets/` and `package*.json` authoring files remain local through the repository's `.gitignore`; the rebuild instructions below apply to this local workspace.

## Rebuild

Use Node.js 22+ on Windows. Arial and Consolas must be available in `C:/Windows/Fonts`. Other systems may set `WADE_FONT_DIR` to a directory containing `arial.ttf`, `arialbd.ttf` and `consola.ttf`; fonts are not bundled.

```powershell
npm install
npm run check
```

`build` regenerates both final exports and `source/NOTES.md` from `source/story-content.cjs`. It checks measured text bounds before replacing exports. `render` creates PDF previews under ignored `.build/`. `validate` checks matching PDF/PPTX text, notes and expected slide order. These checks do not render the PowerPoint through Microsoft Office and do not execute any AWS/emulator example commands.

The renderer can also reuse the existing reference-tool dependencies on the original workspace. Normal checkouts should use `npm install` here.

For a shareable presentation, upload the PDF if preserving appearance is the priority; importing the PPTX allows editing but should be visually checked after conversion. No AI regeneration is needed.
