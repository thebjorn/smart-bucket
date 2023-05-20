import os

LOREM_IPSUM = """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec
a diam lectus. Sed sit amet ipsum mauris. Maecenas congue ligula ac quam
viverra nec consectetur ante hendrerit. Donec et mollis dolor. Praesent
et diam eget libero egestas mattis sit amet vitae augue. Nam tincidunt
congue enim, ut porta lorem lacinia consectetur. Donec ut libero sed"""


def generate_file_tree(root: str, depth: int = 3, count: int = 3):
    """Generate a file tree for testing, with depth of depth and count number
       of files at each level. Fill the files with a paragraph of lorem ipsum.
    """
    os.makedirs(root, exist_ok=True)
    for i in range(count):
        with open(os.path.join(root, f'file{i}.txt'), 'w') as f:
            f.write(LOREM_IPSUM)
    if depth > 0:
        for i in range(count):
            generate_file_tree(os.path.join(root, f'dir{i}'), depth - 1, count)



if __name__ == '__main__':
    generate_file_tree(os.path.join(os.path.dirname(__file__), 'test1'))
