import logging

def has_required_fields(doc, required_fields):
    """
    Check if a CouchDB document contains all the required fields.

    Args:
        doc (dict): The CouchDB document to be validated.
        required_fields (list): A list of field names to be checked in the document.

    Returns:
        bool: True if the document contains all the required fields; False otherwise.
    """
    for req_field in required_fields:
        field_names = req_field.split('.')

        current = doc

        for field_name in field_names:
            if field_name in current:
                current = current[field_name]
            else:
                logging.info(f"Field name '{field_name}' not in Document")
                return False
    return True