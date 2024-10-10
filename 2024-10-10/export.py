import json
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_to_json_line_by_line(data, file_path='agent_details.jsonl'):
    """
    Save data as JSON, each record on a new line.

    Args:
        data (dict): The data to write to the JSON file.
        file_path (str): The path to the output JSON file.
    """
    try:
        with open(file_path, 'a', encoding='utf-8') as file:
            json.dump(data, file)
            file.write('\n')
        logging.info(f'Successfully saved data to {file_path}')
    except Exception as e:
        logging.error(f'Error saving data to JSON: {e}')
