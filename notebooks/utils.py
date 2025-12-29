from pyspark.sql.functions import col, trim, to_timestamp, when
from pyspark.sql.types import StringType, FloatType, IntegerType, TimestampType, DoubleType

def clean_and_enforce_schema(df, target_schema):
    """
    1. Loops through the TARGET schema.
    2. Applies specific cleaning rules based on the DATA TYPE.
    3. Selects only the columns defined in the schema.
    """
    cleaned_cols = []

    for field in target_schema.fields:
        col_name = field.name
        target_type = field.dataType
        
        # Only process if the column actually exists in the input DataFrame
        if col_name in df.columns:
            
            #Handle Strings Trims
            if isinstance(target_type, StringType):
                cleaned_cols.append(trim(col(col_name)).cast(StringType()).alias(col_name))
            
            #Handle Numbers
            elif isinstance(target_type, (FloatType, DoubleType, IntegerType)):
                #If it fails ("abc"), it becomes NULL
                cleaned_cols.append(col(col_name).cast(target_type).alias(col_name))
            
            #Handle Timestamps
            elif isinstance(target_type, TimestampType):
                # to_timestamp
                cleaned_cols.append(to_timestamp(col(col_name)).alias(col_name))
            
            #Fallback for other types
            else:
                cleaned_cols.append(col(col_name).cast(target_type).alias(col_name))
        
    # Apply all transformations at once
    return df.select(cleaned_cols)