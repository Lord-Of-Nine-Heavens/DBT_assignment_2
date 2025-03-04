{% test is_positive(model, column_name) %}

WITH validation AS (

    SELECT
        {{ column_name }} AS positive_field
    FROM {{ model }}

),

validation_errors AS (

    SELECT
        positive_field
    FROM validation

    WHERE positive_field < 0

)

SELECT *
FROM validation_errors

{% endtest %}
