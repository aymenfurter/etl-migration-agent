SELECT
    id,
    SPLIT_PART(name, ' ', 1) AS firstname,
    age
FROM
    input_table;