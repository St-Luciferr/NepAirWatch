"""Capture dashboard screenshots for the report (requires the dashboard on :8501)."""
import sys
from playwright.sync_api import sync_playwright

OUT = sys.argv[1] if len(sys.argv) > 1 else "docs/screenshots"
TABS = ["Live monitor", "Ground stations", "City explorer", "Analytics (batch)", "Fires & aerosols",
        "Early warning (ML)", "News", "Hive SQL", "Pipeline health"]

with sync_playwright() as p:
    # software WebGL, otherwise the Plotly maps render blank in headless Chromium
    b = p.chromium.launch(args=["--use-gl=angle", "--use-angle=swiftshader", "--enable-unsafe-swiftshader"])
    pg = b.new_page(viewport={"width": 1600, "height": 1250})
    pg.goto("http://localhost:8501", wait_until="networkidle")
    pg.wait_for_timeout(15000)
    pg.screenshot(path=f"{OUT}/0_overview.png")  # viewport only: used on the deck's demo slide
    for i, t in enumerate(TABS):
        pg.get_by_role("tab", name=t, exact=False).click()
        pg.wait_for_timeout(7000)
        pg.screenshot(path=f"{OUT}/{i+1}_{t.split()[0].lower()}.png", full_page=True)
        print("saved", t)
    b.close()
