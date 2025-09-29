# tools/list_callbacks.py
import ast, re, sys
from pathlib import Path

src = Path(sys.argv[1]).read_text(encoding="utf-8")
tree = ast.parse(src)

def node_str(n):
    if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
        # Output('id','prop') / Input('id','prop') / State('id','prop')
        name = n.func.attr
        args = [ast.literal_eval(a) for a in n.args] if n.args else []
        return f"{name}({', '.join(map(repr, args))})"
    return None

for n in ast.walk(tree):
    if isinstance(n, ast.FunctionDef):
        for dec in n.decorator_list:
            # cerca @app.callback(...)
            if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute) and dec.func.attr == "callback":
                outs = [node_str(a) for a in dec.args if node_str(a)]
                kws  = []
                for kw in dec.keywords or []:
                    if isinstance(kw.value, ast.List):
                        items = ", ".join([node_str(i) or "?" for i in kw.value.elts])
                        kws.append(f"{kw.arg}=[{items}]")
                print(f"\nCallback: {n.name}")
                if outs: print("  Outputs:", ", ".join(outs))
                if kws:  print("  Kwargs: ", ", ".join(kws))
