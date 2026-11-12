from lab import fetch

def inspect(base, fixture):
    status, headers, body=fetch(base+'/events')
    return {'状态':status,'类型':headers.get('Content-Type'),'原始字节数':len(body),
            '内容':body.decode('utf-8')}
