#!/usr/bin/env python3
"""Generate CZarp brand assets, PWA icons, favicon, and the OG/Twitter share image.

Sources (approved on-palette marks) live in web/brand-src/:
  czarp-on-palette-wordmark.png  — 2000×2000, white + #ff7a1a wordmark on ink #07090d
  czarp-on-palette-avatar.png    — 2000×2000, white CZ mark on ink

The wordmark is cropped to its content bounds and the solid ink background is
keyed out (un-composed) so anti-aliased edges stay clean on the ink page canvas.

Usage (from web/):  python3 scripts/generate-pwa-icons.py
"""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np
from PIL import Image

INK_RGB = (7, 9, 13)
INK = (*INK_RGB, 255)
# Same token as src/lib/pwa.ts / globals.css --color-ink-950.
COURT_500 = (255, 122, 26, 255)  # #ff7a1a — tagline color, kept for comments / checks

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "brand-src"
OUT_ICONS = ROOT / "public" / "icons"
OUT_BRAND = ROOT / "public" / "brand"
APP = ROOT / "src" / "app"
PUBLIC = ROOT / "public"


def uncompose_ink(im: Image.Image, bg=INK_RGB, floor: float = 0.02) -> Image.Image:
    """Key out a solid ink background, recovering un-premultiplied color + alpha.

    For each channel, alpha is how far the pixel has moved from the background
    toward a fully opaque color. Edge pixels keep their anti-aliasing instead of
    a hard cut or a dark halo.
    """
    arr = np.array(im.convert("RGB"), dtype=np.float32)
    bgv = np.array(bg, dtype=np.float32)
    alpha = np.zeros(arr.shape[:2], dtype=np.float32)
    for c in range(3):
        o = arr[:, :, c]
        b = bgv[c]
        contrib = np.where(o >= b, (o - b) / max(255.0 - b, 1.0), (b - o) / max(b, 1.0))
        alpha = np.maximum(alpha, contrib)
    alpha = np.clip(alpha, 0, 1)
    alpha = np.where(alpha < floor, 0, alpha)
    a = np.maximum(alpha, 1e-6)[:, :, None]
    fg = np.clip(bgv + (arr - bgv) / a, 0, 255)
    out = np.dstack([fg, alpha * 255]).astype(np.uint8)
    return Image.fromarray(out, "RGBA")


def crop_to_alpha(im: Image.Image, pad: int = 6, cutoff: int = 8) -> Image.Image:
    a = np.array(im.split()[-1])
    ys, xs = np.where(a > cutoff)
    if xs.size == 0:
        return im
    l, r = int(xs.min()), int(xs.max()) + 1
    t, b = int(ys.min()), int(ys.max()) + 1
    l, t = max(0, l - pad), max(0, t - pad)
    r, b = min(im.width, r + pad), min(im.height, b + pad)
    return im.crop((l, t, r, b))


def fit_inside(im: Image.Image, box: int, box_h: int | None = None) -> Image.Image:
    """Scale `im` to fit inside box×box_h, LANCZOS, no upscale past native."""
    max_w, max_h = box, box if box_h is None else box_h
    w, h = im.size
    scale = min(max_w / w, max_h / h, 1.0) if False else min(max_w / w, max_h / h)
    nw, nh = max(1, int(round(w * scale))), max(1, int(round(h * scale)))
    if (nw, nh) == (w, h):
        return im
    return im.resize((nw, nh), Image.Resampling.LANCZOS)


def height_scale(im: Image.Image, height: int) -> Image.Image:
    w, h = im.size
    nw = max(1, int(round(w * (height / h))))
    return im.resize((nw, height), Image.Resampling.LANCZOS)


def save_png(im: Image.Image, path: Path, *, opaque: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = im.convert("RGB") if opaque else im
    out.save(path, "PNG", optimize=True)
    print(f"wrote {path.relative_to(ROOT)} {out.size} {out.mode} {path.stat().st_size}B")


def save_webp(im: Image.Image, path: Path, *, quality: int = 90) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Lossless for flat typography — stays sharp and stays small.
    im.save(path, "WEBP", lossless=True, method=6, exact=True)
    print(f"wrote {path.relative_to(ROOT)} {im.size} {im.mode} {path.stat().st_size}B")


def save_pair(im: Image.Image, dest_stem: Path) -> None:
    save_png(im, dest_stem.with_suffix(".png"))
    save_webp(im, dest_stem.with_suffix(".webp"))


def place_on_ink(mark: Image.Image, size: int, *, pad_ratio: float) -> Image.Image:
    canvas = Image.new("RGBA", (size, size), INK)
    inner = max(1, size - 2 * int(round(size * pad_ratio)))
    fitted = fit_inside(mark, inner)
    x = (size - fitted.width) // 2
    y = (size - fitted.height) // 2
    canvas.alpha_composite(fitted, (x, y))
    return canvas


def place_maskable(mark: Image.Image, size: int, *, safe_ratio: float = 0.80) -> Image.Image:
    """Full-bleed ink; the CZ mark sits in the center 80% safe zone.

    A square inscribed in that circle has side 0.80/sqrt(2) ≈ 0.566 of the canvas.
    """
    canvas = Image.new("RGBA", (size, size), INK)
    inner = size * safe_ratio / math.sqrt(2) * 0.92
    pad = int(round((size - inner) / 2))
    box = max(1, size - 2 * pad)
    fitted = fit_inside(mark, box)
    x = (size - fitted.width) // 2
    y = (size - fitted.height) // 2
    canvas.alpha_composite(fitted, (x, y))
    return canvas


def save_favicon(src: Image.Image, path: Path) -> None:
    sizes = [(16, 16), (32, 32), (48, 48)]
    src.convert("RGBA").save(path, format="ICO", sizes=sizes)
    print(f"wrote {path.relative_to(ROOT)} ico {sizes} {path.stat().st_size}B")


def compose_og(wordmark: Image.Image, size=(1200, 630)) -> Image.Image:
    canvas = Image.new("RGBA", size, INK)
    # Wordmark at ~52% of canvas width, centered.
    target_w = int(size[0] * 0.52)
    scale = target_w / wordmark.width
    nw, nh = target_w, max(1, int(round(wordmark.height * scale)))
    fitted = wordmark.resize((nw, nh), Image.Resampling.LANCZOS)
    x = (size[0] - fitted.width) // 2
    y = (size[1] - fitted.height) // 2
    canvas.alpha_composite(fitted, (x, y))
    return canvas


def main() -> None:
    wordmark_src = SRC / "czarp-on-palette-wordmark.png"
    avatar_src = SRC / "czarp-on-palette-avatar.png"
    if not wordmark_src.is_file() or not avatar_src.is_file():
        raise SystemExit(f"missing brand sources in {SRC}")

    wordmark = crop_to_alpha(uncompose_ink(Image.open(wordmark_src)))
    mark = crop_to_alpha(uncompose_ink(Image.open(avatar_src)))
    print(f"wordmark cropped {wordmark.size}; mark cropped {mark.size}")

    # Header / hero wordmarks: 1x and 2x, plus a larger hero 2x.
    # Header displays at 28–36px tall; hero at 40–56px.
    save_pair(height_scale(wordmark, 36), OUT_BRAND / "wordmark")
    save_pair(height_scale(wordmark, 72), OUT_BRAND / "wordmark@2x")
    save_pair(height_scale(wordmark, 56), OUT_BRAND / "wordmark-hero")
    save_pair(height_scale(wordmark, 112), OUT_BRAND / "wordmark-hero@2x")

    # "any" purpose PWA icons: ink canvas + cropped CZ with a little padding.
    any_512 = place_on_ink(mark, 512, pad_ratio=0.10)
    any_192 = place_on_ink(mark, 192, pad_ratio=0.10)
    any_48 = place_on_ink(mark, 48, pad_ratio=0.10)
    save_png(any_192, OUT_ICONS / "icon-192.png", opaque=True)
    save_png(any_512, OUT_ICONS / "icon-512.png", opaque=True)
    save_png(any_48, OUT_ICONS / "icon-48.png", opaque=True)

    # Maskable + apple-touch: full-bleed ink, mark inside the 80% safe zone
    # so a circle/squircle crop or iOS rounding does not clip the letters.
    maskable = place_maskable(mark, 512, safe_ratio=0.80)
    save_png(maskable, OUT_ICONS / "icon-512-maskable.png", opaque=True)
    apple = place_maskable(mark, 180, safe_ratio=0.80)
    save_png(apple, OUT_ICONS / "apple-touch-icon.png", opaque=True)
    save_png(apple, PUBLIC / "apple-touch-icon.png", opaque=True)
    save_png(apple, PUBLIC / "apple-touch-icon-precomposed.png", opaque=True)

    # Favicon (multi-size ico) and Next.js app/icon.png.
    save_favicon(any_512, APP / "favicon.ico")
    save_png(place_on_ink(mark, 32, pad_ratio=0.08), APP / "icon.png", opaque=True)

    # OG / Twitter share image.
    og = compose_og(wordmark)
    save_png(og, PUBLIC / "og.png", opaque=True)
    save_png(og, OUT_BRAND / "og.png", opaque=True)


if __name__ == "__main__":
    main()
