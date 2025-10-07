"""Data processing module for CSV file handling and batch management."""

import os
import csv
import logging
from typing import List, Dict, Any
from protocol import DataType # type: ignore

logger = logging.getLogger(__name__)

# Maximum serialized size per row type in bytes (calculated from CSV structure analysis)
ESTIMATED_ROW_SIZES = {
    DataType.TRANSACTIONS: 95,      # transaction_id(40) + store_id(4) + payment_method_id(4) + 
                                    # voucher_id(4) + user_id(4) + amounts(12) + created_at(23) + overhead(4)
    DataType.TRANSACTION_ITEMS: 83,  # transaction_id(40) + item_id(4) + quantity(4) + 
                                     # unit_price(4) + subtotal(4) + created_at(23) + overhead(4)
    DataType.USERS: 56,             # user_id(4) + gender(11) + birthdate(14) + 
                                    # registered_at(23) + overhead(4)
    DataType.STORES: 60,            # store_id(4) + store_name(50) + overhead(6)
    DataType.MENU_ITEMS: 88         # item metadata fields (id, name, category, price, flags)
}


def get_max_row_size(data_type: DataType) -> int:
    """Get the maximum serialized size for a row of the given data type.
    
    Args:
        data_type: The type of data
        
    Returns:
        Maximum size in bytes for a row of this type
    """
    return ESTIMATED_ROW_SIZES.get(data_type, 100)  # Default to 100 if unknown type


def estimate_row_size(data_type: DataType, sample_row: Dict[str, Any] | None = None) -> int:
    """Estimate the serialized size of a row in bytes using hardcoded maximums.
    
    Args:
        data_type: The type of data
        sample_row: Optional sample row (not used in current implementation)
        
    Returns:
        Estimated size in bytes
    """
    # Use hardcoded maximum sizes instead of actual serialization
    # This is more efficient and the sizes are predictable based on CSV structure
    return get_max_row_size(data_type)


class DataProcessor:
    """Handles CSV file processing and batch management."""
    
    def __init__(self, data_dir: str, max_batch_size_kb: int):
        """Initialize data processor.
        
        Args:
            data_dir: Directory containing CSV data files
            max_batch_size_kb: Maximum batch size in KB
        """
        self.data_dir = data_dir
        self.max_batch_size_kb = max_batch_size_kb
        # Convert string environment variable to boolean
        reduced_env = os.getenv('REDUCED', 'true').lower()
        self.reduced = reduced_env in ('true', '1', 'yes', 'on')
    
    def get_csv_files_by_type(self, data_type_str: str) -> List[str]:
        """Get all CSV files for a specific data type.
        
        Args:
            data_type_str: String representation of data type (e.g., 'transactions')
            
        Returns:
            List of file paths for the data type
        """
        type_dir = os.path.join(self.data_dir, data_type_str)

        if not os.path.exists(type_dir):
            logger.warning(f"Directory {type_dir} does not exist")
            return []

        csv_files = []
        for file in os.listdir(type_dir):
            if file.endswith(".csv"):
                csv_files.append(os.path.join(type_dir, file))

        reduce = self.reduced and data_type_str in ['transactions', 'transaction_items']
        if reduce:
            csv_files = [f for f in csv_files if f.endswith("01.csv")] # Solo el mes de enero (2024/5)

        csv_files.sort()  # Process files in order
        logger.info(f"Found {len(csv_files)} CSV files in {type_dir}")
        return csv_files
    
    def calculate_optimal_batch_size(self, data_type: DataType) -> int:
        """Calculate optimal batch size based on KB limit and hardcoded row size.
        
        Args:
            data_type: The type of data to calculate batch size for
            
        Returns:
            Optimal batch size in number of rows
        """
        max_row_size = get_max_row_size(data_type)
        max_batch_size_bytes = self.max_batch_size_kb * 1024
        
        # Calculate how many rows fit in the KB limit
        optimal_batch_size = max(1, int(max_batch_size_bytes / max_row_size))
        
        # Special handling for stores (usually small files)
        if data_type == DataType.STORES:
            # For stores, use a smaller batch size since files are typically small
            optimal_batch_size = min(optimal_batch_size, 100)
        
        # Cap at reasonable maximum to avoid memory issues
        optimal_batch_size = min(optimal_batch_size, 10000)
        
        logger.info(f"Calculated batch size for {data_type.name}: {optimal_batch_size} rows "
                   f"(max row size: {max_row_size} bytes, target: {self.max_batch_size_kb}KB)")
        
        return optimal_batch_size
    
    def process_csv_file_streaming(self, file_path: str, batch_size: int) -> List[List[Dict[str, Any]]]:
        """Stream process CSV file, yielding batches as they're read.
        
        Args:
            file_path: Path to the CSV file
            batch_size: Size of each batch
            
        Returns:
            List of batches (each batch is a list of dictionaries)
        """
        batches = []
        current_batch = []
        
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    # Clean up the row data
                    cleaned_row = {}
                    for key, value in row.items():
                        cleaned_key = key.strip()
                        cleaned_value = value.strip() if isinstance(value, str) else value
                        cleaned_row[cleaned_key] = cleaned_value
                    
                    current_batch.append(cleaned_row)
                    
                    # Create batch when it reaches the target size
                    if len(current_batch) >= batch_size:
                        batches.append(current_batch.copy())
                        current_batch = []
                
                # Add remaining rows as the last batch
                if current_batch:
                    batches.append(current_batch)
            
            logger.debug(f"Processed {sum(len(batch) for batch in batches)} rows from {file_path} into {len(batches)} batches")
            return batches
            
        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")
            return []
    
    def get_data_types_info(self) -> List[tuple[DataType, str]]:
        """Get list of data types and their string representations in processing order.
        
        Returns:
            List of tuples containing (DataType, string_representation)
        """
        return [
            (DataType.USERS, 'users'),
            (DataType.STORES, 'stores'),
            (DataType.MENU_ITEMS, 'menu_items'),
            (DataType.TRANSACTIONS, 'transactions'),
            (DataType.TRANSACTION_ITEMS, 'transaction_items'),
        ]