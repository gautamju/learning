public static JObject GetJsonFieldChangeSummary(JObject oldJson, JObject newJson)
{
    var fieldChangeCounts = new Dictionary<string, int>();
    var modifiedFieldSet = new HashSet<string>();
    int added = 0, deleted = 0, modified = 0;

    var oldItems = oldJson["details"]?.ToObject<List<JObject>>()?.ToDictionary(x => x["id"]?.ToString());
    var newItems = newJson["details"]?.ToObject<List<JObject>>()?.ToDictionary(x => x["id"]?.ToString());

    if (oldItems == null || newItems == null)
        return new JObject();

    var oldKeys = new HashSet<string>(oldItems.Keys);
    var newKeys = new HashSet<string>(newItems.Keys);

    added = newKeys.Except(oldKeys).Count();
    deleted = oldKeys.Except(newKeys).Count();

    foreach (var commonId in oldKeys.Intersect(newKeys))
    {
        var oldObj = oldItems[commonId];
        var newObj = newItems[commonId];

        var allKeys = oldObj.Properties().Select(p => p.Name)
                         .Union(newObj.Properties().Select(p => p.Name))
                         .Distinct();

        bool hasAnyChange = false;

        foreach (var key in allKeys)
        {
            var oldVal = oldObj[key];
            var newVal = newObj[key];

            // Compare arrays of primitive types as unordered
            if (oldVal is JArray oldArr && newVal is JArray newArr)
            {
                var oldSorted = new JArray(oldArr.OrderBy(x => x.ToString()));
                var newSorted = new JArray(newArr.OrderBy(x => x.ToString()));

                if (!JToken.DeepEquals(oldSorted, newSorted))
                {
                    fieldChangeCounts.TryGetValue(key, out int count);
                    fieldChangeCounts[key] = count + 1;
                    modifiedFieldSet.Add(key);
                    hasAnyChange = true;
                }
            }
            else if (!JToken.DeepEquals(oldVal, newVal))
            {
                fieldChangeCounts.TryGetValue(key, out int count);
                fieldChangeCounts[key] = count + 1;
                modifiedFieldSet.Add(key);
                hasAnyChange = true;
            }
        }

        if (hasAnyChange)
            modified++;
    }

    return new JObject
    {
        ["modified_field_counts"] = JObject.FromObject(fieldChangeCounts),
        ["modified_fields"] = new JArray(modifiedFieldSet.OrderBy(x => x)),
        ["added"] = added,
        ["deleted"] = deleted,
        ["total_modified"] = modified
    };
}
