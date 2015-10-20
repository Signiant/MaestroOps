import re

def replaceall(replace_dict, string):
    """
    replaceall will take the keys in replace_dict and replace string with their corresponding values. Keys can be regular expressions.
    """
    replace_dict = dict((re.escape(k), v) for k, v in replace_dict.iteritems())
    pattern = re.compile("|".join(replace_dict.keys()))
    return pattern.sub(lambda m: replace_dict[re.escape(m.group(0))], string)
