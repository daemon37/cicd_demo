#!/usr/bin/env python3
"""
ASCII Mandelbrot Set Generator
Renders the Mandelbrot fractal using ASCII characters in the terminal.
"""

def mandelbrot(c: complex, max_iter: int = 50) -> int:
    """Return the number of iterations until divergence."""
    z = 0
    for n in range(max_iter):
        if abs(z) > 2:
            return n
        z = z * z + c
    return max_iter


def render(width: int = 80, height: int = 24) -> None:
    """Render the Mandelbrot set as ASCII art."""
    chars = " .:-=+*#%@"
    x_min, x_max = -2.5, 1.0
    y_min, y_max = -1.25, 1.25
    
    for y in range(height):
        row = ""
        imag = y_min + (y / (height - 1)) * (y_max - y_min)
        for x in range(width):
            real = x_min + (x / (width - 1)) * (x_max - x_min)
            c = complex(real, imag)
            m = mandelbrot(c)
            idx = int((m / max_iter) * (len(chars) - 1))
            row += chars[idx]
        print(row)


if __name__ == "__main__":
    render()
