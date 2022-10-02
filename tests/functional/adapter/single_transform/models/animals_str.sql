select 
    n_legs::VARCHAR as n_legs,
    animals
from {{ source('test', 'animals') }}
