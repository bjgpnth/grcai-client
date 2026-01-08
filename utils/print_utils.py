# utils/print_utils.py

def divider(char="-", length=60):
    print(char * length)

def print_component_header(name: str):
    divider("=")
    print(f"🔹 Component: {name}")
