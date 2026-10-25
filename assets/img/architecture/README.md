# Story illustrations

`whiteboard-atlas.webp` is the original 3 × 3 illustration sheet.
`story-figures.webp` adds nine transparent illustrations in the same style.
Some of its drawings cross their cell lines, so the slides use `story-figures-cells.webp` and
`whiteboard-atlas-cells.webp`: the same drawings, with every drawing that touches a cell edge re-centred inside
its own cell (cells already clear of their edges are copied unchanged). Regenerate them after editing a source sheet.
All people in both sheets are men, with varied skin tones and outfits.
The original analyst has also been revised to match this preference.
Both sheets use equal square cells, read left to right, top to bottom.

| New figure kind | Subject |
| --- | --- |
| `stakeholders` | Business colleagues discussing a chart |
| `scientist` | Data scientist with laptop and model |
| `operations` | Operations lead with clipboard, parcel and clock |
| `steward` | Governance steward with shield and locked document |
| `customer` | Customer with phone and grocery bag |
| `courier` | Courier on a delivery scooter |
| `payment` | Payment card and receipt |
| `quality` | Magnifying glass checking a data table |
| `monitoring` | Monitor, clock and alert bell |

Use these through the existing story helper, for example:

```js
art('stakeholders', 500, 0, 200, 'The teams', '', 'cover-teams')
art('scientist', 1200, 305, 150, 'Data scientists', 'Explore + ML', 'scientist')
```

Keep captions as HTML so they stay editable and accessible. Artwork is decorative;
its meaning comes from the caption or surrounding text. Keep data IDs stable when
a figure should move between scenes. Classes also work directly on `.art` elements.

The new sheet was created with the built-in image generation tool, using the
original atlas as a style reference. The transparent result was converted to
lossless WebP without resizing. Generation and revision prompts are in `story-figures.prompt.txt` and
`whiteboard-atlas.prompt.txt`.
