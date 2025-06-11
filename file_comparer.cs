using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

class JsonDiffChecker
{
    public static void CompareAllFiles(string folderPath, string outputFolder = null)
    {
        var files = Directory.GetFiles(folderPath, "*.json.gz")
                             .OrderByDescending(File.GetLastWriteTime)
                             .ToList();

        for (int i = 0; i < files.Count - 1; i++)
        {
            var fileA = files[i + 1];
            var fileB = files[i];

            Console.WriteLine($"\n🔍 Comparing: {Path.GetFileName(fileB)} ⟷ {Path.GetFileName(fileA)}");

            var jsonA = LoadGzipJson(fileA);
            var jsonB = LoadGzipJson(fileB);

            var diff = GetJsonDiff(jsonA, jsonB);

            if (!string.IsNullOrEmpty(outputFolder))
            {
                Directory.CreateDirectory(outputFolder);
                var outPath = Path.Combine(outputFolder, $"diff_{i + 1}_{Path.GetFileName(fileB)}_vs_{Path.GetFileName(fileA)}.json");
                File.WriteAllText(outPath, JsonConvert.SerializeObject(diff, Formatting.Indented));
                Console.WriteLine($"✅ Saved diff to {outPath}");
            }
            else
            {
                Console.WriteLine(JsonConvert.SerializeObject(diff, Formatting.Indented));
            }
        }
    }

    private static JObject LoadGzipJson(string filePath)
    {
        using var fs = File.OpenRead(filePath);
        using var gz = new GZipStream(fs, CompressionMode.Decompress);
        using var sr = new StreamReader(gz);
        using var reader = new JsonTextReader(sr);
        return JObject.Load(reader);
    }

    public static JObject GetJsonDiff(JObject oldJson, JObject newJson)
    {
        var result = new JObject
        {
            ["added"] = new JArray(),
            ["deleted"] = new JArray(),
            ["modified"] = new JArray()
        };

        var oldItems = oldJson["details"]?.ToObject<List<JObject>>()?.ToDictionary(x => x["id"]?.ToString());
        var newItems = newJson["details"]?.ToObject<List<JObject>>()?.ToDictionary(x => x["id"]?.ToString());

        if (oldItems == null || newItems == null)
            return result;

        var oldKeys = new HashSet<string>(oldItems.Keys);
        var newKeys = new HashSet<string>(newItems.Keys);

        foreach (var deletedId in oldKeys.Except(newKeys))
            ((JArray)result["deleted"]).Add(new JObject { ["id"] = deletedId, ["data"] = oldItems[deletedId] });

        foreach (var addedId in newKeys.Except(oldKeys))
            ((JArray)result["added"]).Add(new JObject { ["id"] = addedId, ["data"] = newItems[addedId] });

        foreach (var commonId in oldKeys.Intersect(newKeys))
        {
            var changes = GetFieldLevelChanges(oldItems[commonId], newItems[commonId]);
            if (changes.Count > 0)
            {
                result["modified"].Add(new JObject
                {
                    ["id"] = commonId,
                    ["changes"] = changes
                });
            }
        }

        return result;
    }

    private static JObject GetFieldLevelChanges(JToken oldToken, JToken newToken)
    {
        if (oldToken.Type != newToken.Type)
        {
            return new JObject
            {
                ["old"] = oldToken,
                ["new"] = newToken
            };
        }

        if (oldToken.Type == JTokenType.Object)
        {
            var diff = new JObject();
            var oldObj = (JObject)oldToken;
            var newObj = (JObject)newToken;

            var allKeys = oldObj.Properties().Select(p => p.Name)
                                .Union(newObj.Properties().Select(p => p.Name))
                                .Distinct();

            foreach (var key in allKeys)
            {
                var oldVal = oldObj[key];
                var newVal = newObj[key];

                var change = GetFieldLevelChanges(oldVal, newVal);
                if (change.HasValues)
                    diff[key] = change;
            }

            return diff;
        }

        if (oldToken.Type == JTokenType.Array)
        {
            if (!JToken.DeepEquals(oldToken, newToken))
            {
                return new JObject
                {
                    ["old"] = oldToken,
                    ["new"] = newToken
                };
            }
            return new JObject();
        }

        return !JToken.DeepEquals(oldToken, newToken)
            ? new JObject { ["old"] = oldToken, ["new"] = newToken }
            : new JObject();
    }
}


class Program
{
    static void Main(string[] args)
    {
        JsonDiffChecker.CompareAllFiles("C:\\Your\\Json\\Folder", "C:\\Output\\Diffs");
    }
}
