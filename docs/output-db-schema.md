## Database Schema

The framework creates a DuckDB database with the following schema structure:

### Schemas

1. **entity**: Contains standardized entity information
    - `name`: Unique entity names with IDs
    - `address`: Unique addresses with IDs
    - `street`: Unique street information with IDs
    - `street_name`: Unique street names with IDs
    - `name_similarity`: TF-IDF similarity scores between entity names
    - `street_name_similarity`: TF-IDF similarity scores between entity addresses
2. **link**: Contains match information between entities
    - `{entity1}_{entity2}`: Links between entities with match scores
3. **User-defined schemas**: Contains the original data with cleaned fields
    - Tables as defined in your configuration

### Key Tables

#### entity.name

- `entity`: Standardized entity name
- `name_id`: Unique identifier for the entity name


#### entity.address

- `entity`: Standardized address
- `address_id`: Unique identifier for the address


#### entity.street

- `entity`: Standardized street
- `street_id`: Unique identifier for the street


#### entity.name_similarity

- `entity_a`: First entity name
- `entity_b`: Second entity name
- `similarity`: TF-IDF similarity score (0-1)
- `id_a`: ID of first entity
- `id_b`: ID of second entity

#### entity.street_name_similarity
- `entity_a`: First entity address
- `entity_b`: Second entity address
- `similarity`: TF-IDF similarity score (0-1)
- `id_a`: ID of first entity
- `id_b`: ID of second entity


#### link.{entity1}_{entity2}

- `{entity1}_{id1}`: ID from first entity
- `{entity2}_{id2}`: ID from second entity
- Various match columns with binary (0/1) or similarity scores (0-1)
