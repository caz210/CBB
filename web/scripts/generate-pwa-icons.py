#!/usr/bin/env python3
"""Generate CZarp PWA icons from the site logo mark (Logo.tsx).

The header mark is a rounded square with a bottom-right gradient
from --color-court-400 (#ff9a3d) to --color-court-500 (#ff7a1a) and
font-black "CZ" in --color-ink-950 (#07090d). This script paints that
mark; it does not invent a new abbreviation.

Usage (from web/):  python3 scripts/generate-pwa-icons.py
"""
from __future__ import annotations

import math
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

# Tokens from src/app/globals.css @theme — keep in sync with src/lib/pwa.ts
INK_950 = (7, 9, 13, 255)
COURT_400 = (255, 154, 61, 255)  # #ff9a3d
COURT_500 = (255, 122, 26, 255)  # #ff7a1a

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "public" / "icons"
APP = ROOT / "src" / "app"
INTER = Path("/usr/share/fonts/truetype/sand-box/google/Inter/Inter-VariableFont_opsz,wght.ttf")
FALLBACK = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")

# Logo.tsx uses rounded-lg (8px) on an h-8 w-8 (32px) square → 25% radius.
LOGO_RADIUS_RATIO = 8 / 32


def lerp(a, b, t):
    return tuple(int(round(a[i] + (b[i] - a[i]) * t)) for i in range(4))


def fill_br_gradient(im: Image.Image, box, c1, c2) -> None:
    """Bottom-right gradient (Tailwind bg-gradient-to-br) inside box (l,t,r,b)."""
    l, t, r, b = box
    w, h = r - l, b - t
    if w <= 0 or h <= 0:
        return
    denom = (w - 1) + (h - 1)
    px = im.load()
    for y in range(t, b):
        for x in range(l, r):
            tt = 0 if denom == 0 else ((x - l) + (y - t)) / denom
            px[x, y] = lerp(c1, c2, tt)


def rounded_mask(size: int, box, radius: int) -> Image.Image:
    m = Image.new("L", (size, size), 0)
    ImageDraw.Draw(m).rounded_rectangle(box, radius=radius, fill=255)
    return m


def inter_black(px: int) -> ImageFont.FreeTypeFont:
    if INTER.exists():
        font = ImageFont.truetype(str(INTER), px)
        # Optical size max (32) + weight 900 (Black) — matches font-black.
        font.set_variation_by_axes([32, 900])
        return font
    return ImageFont.truetype(str(FALLBACK), px)


def draw_cz(im: Image.Image, box, color=INK_950) -> None:
    """Centered 'CZ' monogram, sized to the logo square."""
    l, t, r, b = box
    side = min(r - l, b - t)
    # Logo uses text-sm (14px) on a 32px square ≈ 44% of the side.
    font_size = max(8, int(side * 0.46))
    font = inter_black(font_size)
    draw = ImageDraw.Draw(im)
    text = "CZ"
    x0, y0, x1, y1 = draw.textbbox((0, 0), text, font=font)
    tw, th = x1 - x0, y1 - y0
    cx = (l + r) / 2
    cy = (t + b) / 2
    # Inter Black sits a hair high visually; nudge down 2% of the side.
    draw.text((cx - tw / 2 - x0, cy - th / 2 - y0 + side * 0.02), text, font=font, fill=color)


def paint_logo_tile(size: int, *, pad_ratio: float, bg) -> Image.Image:
    """Opaque icon: bg canvas + rounded gradient tile + CZ."""
    im = Image.new("RGBA", (size, size), bg)
    pad = int(round(size * pad_ratio))
    box = (pad, pad, size - pad, size - pad)
    side = box[2] - box[0]
    radius = max(1, int(round(side * LOGO_RADIUS_RATIO)))
    layer = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    fill_br_gradient(layer, box, COURT_400, COURT_500)
    mask = rounded_mask(size, box, radius)
    tile = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    tile.paste(layer, mask=mask)
    im.alpha_composite(tile)
    draw_cz(im, box)
    return im


def paint_full_bleed(size: int, *, safe_ratio: float = 0.80) -> Image.Image:
    """Full-bleed gradient; CZ stays inside the maskable / iOS safe zone.

    Maskable safe zone is the center 80%. A square inscribed in that circle
    has side 0.80/sqrt(2) ≈ 0.566 of the canvas; we use that for the letters.
    """
    im = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    fill_br_gradient(im, (0, 0, size, size), COURT_400, COURT_500)
    # Letter box: inscribed square of the safe-zone circle, with a little air.
    safe = size * safe_ratio
    inner = safe / math.sqrt(2) * 0.92
    pad = int(round((size - inner) / 2))
    draw_cz(im, (pad, pad, size - pad, size - pad))
    return im


def save_png(im: Image.Image, path: Path, *, opaque: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = im.convert("RGB") if opaque else im
    out.save(path, "PNG", optimize=True)
    print(f"wrote {path.relative_to(ROOT)} {out.size} {out.mode}")


def save_favicon(src: Image.Image, path: Path) -> None:
    sizes = [(16, 16), (32, 32), (48, 48)]
    src.convert("RGBA").save(path, format="ICO", sizes=sizes)
    print(f"wrote {path.relative_to(ROOT)} ico {sizes}")


def main() -> None:
    # "any" purpose: same composition as the header mark (ink canvas + rounded tile).
    any_512 = paint_logo_tile(512, pad_ratio=0.08, bg=INK_950)
    any_192 = paint_logo_tile(192, pad_ratio=0.08, bg=INK_950)
    save_png(any_192, OUT / "icon-192.png", opaque=True)
    save_png(any_512, OUT / "icon-512.png", opaque=True)

    # Maskable: full-bleed court gradient so a circle/squircle crop stays on-brand;
    # CZ is padded into the 80% safe zone.
    maskable = paint_full_bleed(512, safe_ratio=0.80)
    save_png(maskable, OUT / "icon-512-maskable.png", opaque=True)

    # apple-touch-icon: 180px, no transparency. iOS rounds the icon, so full-bleed
    # (same as maskable) avoids a dark gutter under the iOS corner mask.
    apple = paint_full_bleed(180, safe_ratio=0.80)
    save_png(apple, OUT / "apple-touch-icon.png", opaque=True)
    # iOS also requests these at the site root (not just /icons/).
    save_png(apple, ROOT / "public" / "apple-touch-icon.png", opaque=True)
    save_png(apple, ROOT / "public" / "apple-touch-icon-precomposed.png", opaque=True)

    # Replace the default Next/Vercel triangle favicon with the same mark.
    save_favicon(any_512, APP / "favicon.ico")


if __name__ == "__main__":
    main()
