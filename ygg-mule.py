#!/usr/bin/env python
import asyncio
import argparse

from lib.utils.config_loader import ConfigLoader
from lib.utils.common import YggdrasilUtilities as Ygg
from lib.couchdb.manager import ProjectDBManager, YggdrasilDBManager
from lib.utils.logging_utils import configure_logging, custom_logger

# Configure logging
configure_logging(debug=True)
logging = custom_logger("Ygg-Mule")

def process_document(doc_id):
    """
    Processes a document by its ID. Fetches the document from the database,
    determines the appropriate module to load, and executes the module.

    Args:
        doc_id (str): The ID of the document to process.
    """
    # Initialize the ProjectDBManager and YggdrasilDBManager
    pdm = ProjectDBManager()
    ydm = YggdrasilDBManager()

    # Fetch the document using the ProjectDBManager
    document = pdm.fetch_document_by_id(doc_id)
    # document = fetch_document_from_db(doc_id)

    if not document:
        logging.error(f"Document with ID {doc_id} not found.")
        return

    project_id = document.get('project_id')

    # Check if the project exists in the yggdrasil database
    existing_document = ydm.check_project_exists(project_id)

    if existing_document is None:
        projects_reference = document.get('_id')
        method = document.get('details', {}).get('library_construction_method')

        # Create a new project if it doesn't exist
        ydm.create_project(project_id, projects_reference, method)
        process_project = True
    else:
        # If the project exists, check if it is completed
        if existing_document.get('status') == 'completed':
            logging.info(f"Project with ID {project_id} is already completed. Skipping further processing.")
            process_project = False
        else:
            logging.info(f"Project with ID {project_id} is ongoing and will be processed.")
            process_project = True

    if process_project:
        # Determine the appropriate module to load
        module_loc = get_module_location(document)

        # Load and execute the module
        try:
            RealmClass = Ygg.load_realm_class(module_loc)
            if RealmClass:
                realm = RealmClass(document)
                if realm.proceed:
                    asyncio.run(realm.process())
                    logging.info("Processing complete.")
                else:
                    logging.info(f"Skipping processing due to missing required information. {document.get('project_id')}")
            else:
                logging.warning(f"Failed to load module '{module_loc}'.")
        except Exception as e:
            logging.error(f"Error while processing document: {e}", exc_info=True)


def get_module_location(document):
    """
    Retrieves the module location based on the library construction method from the document.

    Args:
        document (dict): The document containing details about the library construction method.

    Returns:
        str: The module location, or None if not found.
    """
    try:
        # Load the module registry configuration
        module_registry = ConfigLoader().load_config("module_registry.json")

        # Extract the library construction method from the document
        method = document['details']['library_construction_method']

        # Retrieve module configuration for the specified method
        module_config = module_registry.get(method)

        if module_config:
            # Return the module location
            return module_config["module"]
        else:
            logging.warning(f"No module configuration found for method '{method}'.")
            return None
    except KeyError as e:
        logging.error(f'Error accessing module location: {e}')
        return None
    except Exception as e:
        logging.error(f'Unexpected error: {e}')
        return None


def main():
    logging.info("Ygg-Mule: Standalone Module Executor for Yggdrasil")
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Ygg-Mule: Standalone Module Executor for Yggdrasil')
    parser.add_argument('doc_id', type=str, help='Document ID to process')

    # Parse arguments
    args = parser.parse_args()

    # Process the document
    process_document(args.doc_id)

if __name__ == "__main__":
    main()
