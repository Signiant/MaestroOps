import re

def replaceall(replace_dict, string):
    replace_dict = dict((re.escape(k), v) for k, v in replace_dict.iteritems())
    pattern = re.compile("|".join(replace_dict.keys()))
    return pattern.sub(lambda m: replace_dict[re.escape(m.group(0))], string)
