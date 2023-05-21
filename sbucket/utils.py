from ruamel import yaml


def _simplify_tree(tree):
    if not isinstance(tree, dict):
        return tree
    if all(v == "" for v in tree.values()):
        return list(tree.keys())
    return {key: _simplify_tree(value) for key, value in tree.items()}


def files2tree(files):
    """Return a file tree from the files.
    """
    res = {}
    for filename in files:
        parts = str(filename).replace('\\', '/').split('/')
        cur = res
        for part in parts[:-1]:
            cur.setdefault(part, {})
            cur = cur[part]
        cur[parts[-1]] = ""

    return _simplify_tree(res)


def tree2yaml(tree):
    """Return a YAML representation of the file tree.
    """
    return yaml.dump(tree, default_flow_style=False, indent=4)
