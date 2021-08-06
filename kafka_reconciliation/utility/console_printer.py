import logging

header_text_format = "\033[95m"
blue_text_format = "\033[94m"
green_text_format = "\033[92m"
warning_text_format = "\033[93m"
error_text_format = "\033[91m"
end_text_formatting = "\033[0m"
bold_text_format = "\033[1m"
underline_text_format = "\033[4m"
italic_text_format = "\033[3m"

all_formats = [
    header_text_format,
    blue_text_format,
    green_text_format,
    warning_text_format,
    error_text_format,
    end_text_formatting,
    bold_text_format,
    underline_text_format,
    italic_text_format,
]


def set_log_level_info():
    logging.basicConfig(level=logging.INFO)


def generate_header(text):
    return f"{header_text_format}{text}{end_text_formatting}"


def generate_bold_text(text):
    return f"{bold_text_format}{text}{end_text_formatting}"


def generate_underline_text(text):
    return f"{underline_text_format}{text}{end_text_formatting}"


def generate_warning_text(text):
    return f"{warning_text_format}{text}{end_text_formatting}"


def generate_error_text(text):
    return f"{error_text_format}{text}{end_text_formatting}"


def generate_italic_text(text):
    return f"{italic_text_format}{text}{end_text_formatting}"


def strip_formatting(text):
    final_text = text
    for text_format in all_formats:
        final_text = final_text.replace(f"{text_format}", "")

    return final_text
