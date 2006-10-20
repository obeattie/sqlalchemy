import sys, re
from toc import TOCElement
import gen_docstrings

try:
    import elementtree.ElementTree as et
except:
    raise "This module requires ElementTree to run (http://effbot.org/zone/element-index.htm)"

sys.path.insert(0, './lib')
import markdown

root = TOCElement('', 'root', '')

def dump_tree(elem, stream):
    if elem.tag.startswith('MYGHTY:'):
        dump_myghty_tag(elem, stream)
    else:
        stream.write("<%s>" % elem.tag)
        if elem.text:
            stream.write(elem.text)
        for child in elem:
            dump_tree(child, stream)
            if child.tail:
                stream.write(child.tail)
        stream.write("</%s>" % elem.tag)

def dump_myghty_tag(elem, stream):
    tag = elem.tag[7:]
    params = ', '.join(['%s="%s"' % i for i in elem.items()])
    pipe = ''
    if elem.text or len(elem):
        pipe = '|'
    comma = ''
    if params:
        comma = ', '
    stream.write('<&%s%s%s%s&>' % (pipe, tag, comma, params))
    if pipe:
        if elem.text:
            stream.write(elem.text)
        for n in elem:
            dump_tree(n, stream)
            if n.tail:
                stream.write(n.tail)
        stream.write("</&>")

def create_toc(filename, tree):
    title = [None]
    current = [root]
    level = [0]
    def process(tree):
        while True:
            i = find_header_index(tree)
            if i is None:
                return
            node = tree[i]
            taglevel = int(node.tag[1])
            start, end = i, end_of_header(tree, taglevel, i+1)
            content = tree[start+1:end]
            description = node.text.strip()
            if title[0] is None:
                title[0] = description
            name = node.get('name')
            if name is None:
                name = description.split()[0].lower()
            
            taglevel = node.tag[1]
            if taglevel > level[0]:
                current[0] = TOCElement(filename, name, description, current[0])
            elif taglevel == level[0]:
                current[0] = TOCElement(filename, name, description, current[0].parent)
            else:
                current[0] = TOCElement(filename, name, description, current[0].parent.parent)

            level[0] = taglevel

            tag = et.Element("MYGHTY:doclib.myt:section", name=name, description=description)
            tag.text = (node.tail or "") + '\n'
            tag.tail = '\n'
            tag[:] = content
            tree[start:end] = [tag]

            process(tag)

    process(tree)
    return title[0]

def index(parent, item):
    for n, i in enumerate(parent):
        if i is item:
            return n

def find_header_index(tree):
    for i, node in enumerate(tree):
        if is_header(node):
            return i

def is_header(node):
    t = node.tag
    return (isinstance(t, str) and len(t) == 2 and t[0] == 'h' 
            and t[1] in '123456789')

def end_of_header(tree, level, start):
    for i, node in enumerate(tree[start:]):
        if is_header(node) and int(node.tag[1]) <= level:
            return start + i
    return len(tree)

def replace_pre_with_myt(tree):
    def splice_code_tag(pre, text, type=None, title=None):
        doctest_directives = re.compile(r'#\s*doctest:\s*[+-]\w+(,[+-]\w+)*\s*$', re.M)
        text = re.sub(doctest_directives, '', text)
        # process '>>>' to have quotes around it, to work with the myghty python
        # syntax highlighter which uses the tokenize module
        text = re.sub(r'>>> ', r'">>>" ', text)

        # indent two spaces.  among other things, this helps comment lines "#  " from being 
        # consumed as Myghty comments.
        text = re.compile(r'^(?!<&)', re.M).sub('  ', text)

        sqlre = re.compile(r'{sql}(.*?)((?:SELECT|INSERT|DELETE|UPDATE|CREATE|DROP|PRAGMA|DESCRIBE).*?)\n\s*(\n|$)', re.S)
        if sqlre.search(text) is not None:
            use_sliders = False
        else:
            use_sliders = True
        
        text = sqlre.sub(r"<&formatting.myt:poplink&>\1\n<&|formatting.myt:codepopper, link='sql'&>\2</&>\n\n", text)

        sqlre2 = re.compile(r'{opensql}(.*?)((?:SELECT|INSERT|DELETE|UPDATE|CREATE|DROP).*?)\n\s*(\n|$)', re.S)
        text = sqlre2.sub(r"<&|formatting.myt:poppedcode &>\1\n\2</&>\n\n", text)

        opts = {}
        if type == 'python':
            opts['syntaxtype'] = 'python'
        else:
            opts['syntaxtype'] = None

        if title is not None:
            opts['title'] = title
    
        if use_sliders:
            opts['use_sliders'] = True
    
        tag = et.Element("MYGHTY:formatting.myt:poplink", **opts)
        tag.text = text

        pre_parent = parents[pre]
        tag.tail = pre.tail
        pre_parent[reverse_parent(pre_parent, pre)] = tag

    parents = get_parent_map(tree)

    for precode in tree.findall('.//pre/code'):
        m = re.match(r'\{(python|code)(?: title="(.*?)"){0,1}\}', precode.text.lstrip())
        if m:
            code = m.group(1)
            title = m.group(2)
            text = precode.text.lstrip()
            text = re.sub(r'{(python|code).*?}(\n\s*)?', '', text)
            splice_code_tag(parents[precode], text, type=code, title=title)
        elif precode.text.lstrip().startswith('>>> '):
            splice_code_tag(parents[precode], precode.text)

def reverse_parent(parent, item):
    for n, i in enumerate(parent):
        if i is item:
            return n

def get_parent_map(tree):
    return dict([(c, p) for p in tree.getiterator() for c in p])


    
if __name__ == '__main__':
    import glob

    docstring_data = gen_docstrings.create_docstring_toc('content/compiled_docstrings.pickle', root)
    gen_docstrings.gen(docstring_data, root)
    if False:
        filenames = sys.argv[1:]
        if len(filenames) == 0:
            filenames = glob.glob('content/*.txt')
        for inname in filenames:
            html = markdown.markdown(file(inname).read())
            tree = et.fromstring("<html>" + html + "</html>")
            create_toc(inname, tree)
            replace_pre_with_myt(tree)
            dump_tree(tree, sys.stdout)
        
