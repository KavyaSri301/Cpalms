import re
def convert_markdown_to_bold_html(text: str) -> str:
    if not text:
        return ""
    header_split = re.split(r'(Description:.*?\n)', text, flags=re.DOTALL)
    if len(header_split) < 3:
        header = text
        body = ''
    else:
        header = ''.join(header_split[:2])  
        body = ''.join(header_split[2:])   

    header = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', header)
    header= re.sub(r'^\s*#{1,6}\s*(.*)', r'<b>\1</b>', header, flags=re.MULTILINE)
    header = re.sub(r'\[(.*?)\]\((.*?)\)', r'<a href="\2" target="_blank">\1</a>', header)
    header = header.replace("**", "").replace("#", "")
    header = re.sub(r'\n\s*\n+', '\n', header)
    header=header.replace("\n", "<br>")
    body = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', body)
    body = re.sub(r'^\s*#{1,6}\s*(.*)', r'<b>\1</b>', body, flags=re.MULTILINE)
    body = re.sub(r'\[(.*?)\]\((.*?)\)', r'<a href="\2" target="_blank">\1</a>', body)
    body= re.sub(r'\n\s*\n+', '\n', body)
    body = re.sub(r'(?<!^)(?<!<br>)\n(?=<b>)', r'<br><br>', body)
    body = body.replace("*", "")
    body = body.replace("#", "")
    body = body.replace('\n', '<br>')
    return header + body

def convert_markdown_to_bold_html_1(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'^\s*#{1,6}\s*(.*)', r'<b>\1</b>', text, flags=re.MULTILINE)
    text = re.sub(r'\[(.*?)\]\((.*?)\)', r'<a href="\2" target="_blank">\1</a>', text)
    text = re.sub(r'(?<!^)(?<!<br>)\n(?=<b>)', r'<br><br>', text)

    text = text.replace("*", "")
    text = text.replace("#", "")
    text = text.replace('\n', '<br>')

    return text



def convert_markdown_to_clean_text(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r'(?<!\n)\s*(^#{1,6}\s*)', r'\n\n\1', text, flags=re.MULTILINE)
    text = re.sub(r'^\s*#{1,6}\s*(.*)', lambda m: f"{m.group(1).strip().upper()}:", text, flags=re.MULTILINE)
    text = re.sub(r'(?<!\n)\n?(?=^\s*\*\*[^\n]+?:\*\*)', r'\n\n', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*(.*?)\*\*', lambda m: m.group(1).upper(), text)
    text = re.sub(r'(?<!\n)\n?(?=^[A-Z\s]+:)', r'\n\n', text, flags=re.MULTILINE)
    text = re.sub(r'\[(.*?)\]\((.*?)\)', r'\1 (\2)', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    text = text.replace("*", "").replace("#", "").strip()

    return text

def convert_markdown_to_clean_text_for_docs(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r'(?<!\n)\s*(^#{1,6}\s*)', r'\n\n\1', text, flags=re.MULTILINE)
    text = re.sub(r'^\s*#{1,6}\s*(.*)', r'**\1**', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*(.*?)\*\*', lambda m: f"**{m.group(1).strip()}**", text)
    text = re.sub(r'(?<!\n)\n?(?=^\*\*.*?\*\*:)', r'\n\n', text, flags=re.MULTILINE)
    text = re.sub(r'\[(.*?)\]\((.*?)\)', r'\1 (\2)', text)
    text = re.sub(r'(?<!\*)\*(?!\*)(.*?)', r'\1', text)  
    text = text.replace("#", "")
    text = re.sub(r'\n\s*\n+', '\n\n', text)

    return text.strip()
