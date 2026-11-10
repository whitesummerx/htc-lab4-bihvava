"""Крок 2 задачі: синтетичний зріз S x S і розтягування контрасту (чистий Python, без NumPy/Numba)."""
import math

LEVELS = 4096


def make_slice(seed: int, n: int):
    """ЛКГ: x(0)=seed, x(i+1)=(1103515245*x(i)+12345) mod 2^31, p(i)=x(i+1)>>19 (значення 0..4095)."""
    x = seed & 0x7FFFFFFF
    out = [0] * n
    for i in range(n):
        x = (1103515245 * x + 12345) & 0x7FFFFFFF
        out[i] = x >> 19
    return out


def quantile_level(hist, n: int, q: float) -> int:
    """Найменший рівень v, для якого накопичена кількість пікселів >= q*n."""
    target = q * n
    acc = 0
    for v in range(LEVELS):
        acc += hist[v]
        if acc >= target:
            return v
    return LEVELS - 1


def process(seed: int, size: int, q_lo: float, q_hi: float) -> dict:
    n = size * size
    pix = make_slice(seed, n)
    hist = [0] * LEVELS
    for p in pix:
        hist[p] += 1
    p_lo = quantile_level(hist, n, q_lo)
    p_hi = quantile_level(hist, n, q_hi)
    span = p_hi - p_lo
    zeroed = 0
    kept = 0
    s1 = 0
    s2 = 0
    for p in pix:
        if p < p_lo or p > p_hi:
            zeroed += 1
        else:
            # округлення до найближчого цілого (половини вгору)
            v = int((4095 * (p - p_lo)) / span + 0.5) if span > 0 else 0
            kept += 1
            s1 += v
            s2 += v * v
    if kept:
        mean = s1 / kept
        var = max(s2 / kept - mean * mean, 0.0)
    else:
        mean = 0.0
        var = 0.0
    return {
        "p_lo": p_lo,
        "p_hi": p_hi,
        "zeroed": zeroed,
        "mean": round(mean, 6),
        "std": round(math.sqrt(var), 6),
    }
