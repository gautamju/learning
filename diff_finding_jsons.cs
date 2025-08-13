using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

public enum ChangeType { Unchanged, Modified, New, Deleted }

public record DiffResult(
    Dictionary<string, ChangeType> ById,
    List<string> NewIds,
    List<string> DeletedIds,
    List<string> ModifiedIds,
    List<string> UnchangedIds);

public class JsonDiffById
{
    public string IdField { get; }
    public bool IgnoreArrayOrder { get; }
    public HashSet<string> IgnoreFields { get; }

    public JsonDiffById(string idField = "id", bool ignoreArrayOrder = false, IEnumerable<string>? ignoreFields = null)
    {
        IdField = idField;
        IgnoreArrayOrder = ignoreArrayOrder;
        IgnoreFields = new HashSet<string>(ignoreFields ?? Enumerable.Empty<string>(), StringComparer.OrdinalIgnoreCase);
        IgnoreFields.Add(IdField); // don't let the id participate in "modified" checks
    }

    public DiffResult CompareFiles(string oldPath, string newPath)
    {
        var oldItems = LoadAndIndex(oldPath);
        var newItems = LoadAndIndex(newPath);

        var allIds = new HashSet<string>(oldItems.Keys, StringComparer.OrdinalIgnoreCase);
        allIds.UnionWith(newItems.Keys);

        var byId = new Dictionary<string, ChangeType>(StringComparer.OrdinalIgnoreCase);
        var news = new List<string>();
        var dels = new List<string>();
        var mods = new List<string>();
        var sames = new List<string>();

        foreach (var id in allIds)
        {
            var inOld = oldItems.TryGetValue(id, out var oldObj);
            var inNew = newItems.TryGetValue(id, out var newObj);

            if (inOld && !inNew)
            {
                byId[id] = ChangeType.Deleted;
                dels.Add(id);
            }
            else if (!inOld && inNew)
            {
                byId[id] = ChangeType.New;
                news.Add(id);
            }
            else
            {
                // present in both: deep-compare after normalization
                var normOld = Normalize(oldObj!);
                var normNew = Normalize(newObj!);

                if (JToken.DeepEquals(normOld, normNew))
                {
                    byId[id] = ChangeType.Unchanged;
                    sames.Add(id);
                }
                else
                {
                    byId[id] = ChangeType.Modified;
                    mods.Add(id);
                }
            }
        }

        return new DiffResult(byId, news, dels, mods, sames);
    }

    private Dictionary<string, JObject> LoadAndIndex(string path)
    {
        using var sr = File.OpenText(path);
        using var jr = new JsonTextReader(sr) { DateParseHandling = DateParseHandling.None };
        var root = JToken.ReadFrom(jr);

        // Support: array of objects OR object-map keyed by id
        IEnumerable<JObject> objects = root switch
        {
            JArray arr => arr.OfType<JObject>(),
            JObject obj => obj.Properties().Select(p => p.Value).OfType<JObject>(),
            _ => throw new InvalidOperationException("Top-level JSON must be an array or an object.")
        };

        var dict = new Dictionary<string, JObject>(StringComparer.OrdinalIgnoreCase);
        foreach (var o in objects)
        {
            var idToken = o[IdField];
            if (idToken == null || idToken.Type == JTokenType.Null)
                throw new InvalidOperationException($"Object missing required id field '{IdField}'. Object: {o}");

            var id = idToken.Type switch
            {
                JTokenType.Integer or JTokenType.Float => idToken.ToString(), // normalize numeric ids to string
                _ => idToken.Value<string>() ?? throw new InvalidOperationException($"Invalid id for object: {o}")
            };

            if (!dict.TryAdd(id, o))
                throw new InvalidOperationException($"Duplicate id '{id}' encountered.");
        }

        return dict;
    }

    private JToken Normalize(JObject obj)
    {
        // Remove ignored fields at the root and recursively normalize
        var cloned = (JObject)obj.DeepClone();
        foreach (var f in IgnoreFields)
        {
            cloned.Remove(f);
        }
        return NormalizeToken(cloned);
    }

    private JToken NormalizeToken(JToken token)
    {
        switch (token.Type)
        {
            case JTokenType.Object:
            {
                var obj = (JObject)token;
                // normalize children first
                var normalizedChildren = obj.Properties()
                    .Select(p => new JProperty(p.Name, NormalizeToken(p.Value)))
                    .ToList();

                // sort properties by name for stable comparison
                var sorted = new JObject(normalizedChildren.OrderBy(p => p.Name, StringComparer.Ordinal));
                return sorted;
            }
            case JTokenType.Array:
            {
                var arr = (JArray)token;
                var normalizedItems = arr.Select(NormalizeToken).ToList();
                if (IgnoreArrayOrder)
                {
                    // Order by string representation of normalized items for order-insensitive compare
                    var sorted = normalizedItems
                        .OrderBy(t => t.ToString(Formatting.None), StringComparer.Ordinal)
                        .ToList();
                    return new JArray(sorted);
                }
                return new JArray(normalizedItems);
            }
            default:
                return token; // primitives fine as-is
        }
    }
}

public static class Program
{
    public static void Main(string[] args)
    {
        // Usage:
        // dotnet run -- old.json new.json [idField] [ignoreArrayOrder true|false] [ignoreField1,ignoreField2,...]
        if (args.Length < 2)
        {
            Console.Error.WriteLine("Usage: <exe> <old.json> <new.json> [idField=id] [ignoreArrayOrder=false] [ignoreFields=updatedAt,version]");
            Environment.Exit(1);
        }

        var oldPath = args[0];
        var newPath = args[1];
        var idField = args.Length >= 3 ? args[2] : "id";
        var ignoreArrayOrder = args.Length >= 4 && bool.TryParse(args[3], out var b) ? b : false;
        var ignoreFields = args.Length >= 5 && !string.IsNullOrWhiteSpace(args[4])
            ? args[4].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            : Array.Empty<string>();

        var diff = new JsonDiffById(idField, ignoreArrayOrder, ignoreFields).CompareFiles(oldPath, newPath);

        Console.WriteLine($"New: {diff.NewIds.Count}");
        Console.WriteLine($"Deleted: {diff.DeletedIds.Count}");
        Console.WriteLine($"Modified: {diff.ModifiedIds.Count}");
        Console.WriteLine($"Unchanged: {diff.UnchangedIds.Count}");
        Console.WriteLine();

        void PrintBucket(string title, IEnumerable<string> ids)
        {
            Console.WriteLine($"== {title} ==");
            foreach (var id in ids.OrderBy(x => x, StringComparer.OrdinalIgnoreCase))
                Console.WriteLine(id);
            Console.WriteLine();
        }

        PrintBucket("NEW", diff.NewIds);
        PrintBucket("DELETED", diff.DeletedIds);
        PrintBucket("MODIFIED", diff.ModifiedIds);
        PrintBucket("UNCHANGED", diff.UnchangedIds);
    }
}
