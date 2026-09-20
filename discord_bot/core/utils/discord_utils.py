from re import sub


def discord_format_string_embed(stringy: str) -> str:
    '''
    Format discord string so it is not embedded
    '''
    # Regex to match URLs and wrap them in angle brackets to prevent embedding
    # This matches https:// followed by non-whitespace characters
    url_pattern = r'(https://\S+)'
    return sub(url_pattern, r'<\1>', stringy)
