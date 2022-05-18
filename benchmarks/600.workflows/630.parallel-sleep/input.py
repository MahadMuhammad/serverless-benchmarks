size_generators = {
    'test' : (5, 0),
    'small': (50, 2),
    'large': (100, 1),
}

def buckets_count():
    return (0, 0)

def generate_input(data_dir, size, input_buckets, output_buckets, upload_func):
    count, sleep = size_generators[size]
    return { 'count': count, 'sleep': sleep }