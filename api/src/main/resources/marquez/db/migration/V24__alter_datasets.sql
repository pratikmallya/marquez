ALTER TABLE datasets ADD namespace_name varchar;
UPDATE datasets SET
namespace_name = n.name
FROM namespaces n
WHERE n.uuid = datasets.namespace_uuid;