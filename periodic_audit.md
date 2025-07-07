④ Periodic Audits & Promotion from JSONB
Every month (or as needed), run a quick audit query to see frequent JSON fields:


SELECT jsonb_object_keys(additional_info) AS key, COUNT(*)
FROM contacts
GROUP BY key
ORDER BY COUNT(*) DESC;

Decide which frequently appearing JSON fields to "promote" to standard columns.

Alter the database schema accordingly.

✅ Why?

Maintain clean data organization over the long term.