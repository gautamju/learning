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
                var outPath = Path.Combine(outputFolder, $"diff_{i + 1}_{Path.GetFileNameWithoutExtension(fileB)}_vs_{Path.GetFileNameWithoutExtension(fileA)}.json");
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
            var changes = GetTopLevelChanges(oldItems[commonId], newItems[commonId]);
            if (changes.HasValues)
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

    private static JObject GetTopLevelChanges(JObject oldObj, JObject newObj)
    {
        var changes = new JObject();
        var allKeys = oldObj.Properties().Select(p => p.Name)
                         .Union(newObj.Properties().Select(p => p.Name))
                         .Distinct();

        foreach (var key in allKeys)
        {
            var oldVal = oldObj.ContainsKey(key) ? oldObj[key] : null;
            var newVal = newObj.ContainsKey(key) ? newObj[key] : null;

            if (!JToken.DeepEquals(oldVal, newVal))
            {
                changes[key] = new JObject
                {
                    ["old"] = oldVal,
                    ["new"] = newVal
                };
            }
        }

        return changes;
    }
}

class Program
{
    static void Main(string[] args)
    {
        // Replace with your actual folder path
        string folderPath = @"C:\Path\To\JsonFiles";
        string outputFolder = @"C:\Path\To\DiffResults";

        JsonDiffChecker.CompareAllFiles(folderPath, outputFolder);
    }
}
