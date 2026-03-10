def build_html() -> str:
    with open("analyzer/page_html.html", "r") as f:
        return f.read()
