--Query to get most used drawing tool
SELECT 
    JSON_EXTRACT(parsed_content, '$.state.type') AS type,
    COUNT(*) AS count
FROM tv_drawings
WHERE JSON_EXTRACT(parsed_content, '$.state.type') IS NOT NULL
GROUP BY JSON_EXTRACT(parsed_content, '$.state.type')
ORDER BY count DESC
Limit 20;

--Query to get most used indicators
SELECT
    json_extract(source_item.value, '$.metaInfo.name') AS name,
    COUNT(*) AS count
FROM tv_study_templates,
    json_each(json_extract(parsed_content, '$.panes')) AS pane_item,
    json_each(json_extract(pane_item.value, '$.sources')) AS source_item
WHERE json_extract(source_item.value, '$.metaInfo.name') IS NOT NULL
GROUP BY json_extract(source_item.value, '$.metaInfo.name')
ORDER BY count DESC;
