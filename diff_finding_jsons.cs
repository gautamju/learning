using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

public enum ChangeType { Unchanged, Modified, New, Deleted }

public sealed class DiffResult
{
    public Dictionary<string, ChangeType> ById;
    public List<string> NewIds;
    public List<string> DeletedIds;
    public List<string> ModifiedIds;
    public List<string> UnchangedIds;

    public DiffResult()
    {
        ById = new Dictionary<string, ChangeType>(StringComparer.OrdinalIgnoreCase);
        NewIds = new List<string>();
        DeletedIds = new List<string>();
        ModifiedIds = new List<string>();
        UnchangedIds = new List<string>();
    }
}

public sealed class JsonDiffById
{
    private readonly string _idField;
    private readonly bool _ignoreArrayOrder;
    private readonly HashSet<string> _ignoreFields;

    public JsonDiffById(string idField, bool ignoreArrayOrder, IEnumerable<string> ignoreFields)
    {
        _idField = string.IsNullOrEmpty(idField) ? "id" : idField;
        _ignoreArrayOrder = ignoreArrayOrder;
        _ignoreFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (ignoreFields != null)
        {
            foreach (var f in ignoreFields) _ignoreFields.Add(f);
        }
        // Don’t let the id participate in modification checks
        _ignoreFields.Add(_idField);
    }

    public DiffResult CompareFiles(string oldPath, string newPath)
    {
        var oldItems = LoadAndIndex(oldPath);
        var newItems = LoadAndIndex(newPath);

        var allIds = new HashSet<string>(oldItems.Keys, StringComparer.OrdinalIgnoreCase);
        foreach (var k in newItems.Keys) allIds.Add(k);

        var result = new DiffResult();

        foreach (var id in allIds)
        {
            JObject oldObj;
            JObject newObj;
            bool inOld = oldItems.TryGetValue(id, out oldObj);
            bool inNew = newItems.TryGetValue(id, out newObj);

            if (inOld && !inNew)
            {
                result.ById[id] = ChangeType.Deleted;
                result.DeletedIds.Add(id);
            }
            else if (!inOld && inNew)
            {
                result.ById[id] = ChangeType.New;
                result.NewIds.Add(id);
            }
            else
            {
                // present in both
                JToken normOld = Normalize(RemoveIgnored((JObject)oldObj.DeepClone()));
                JToken normNew = Normalize(RemoveIgnored((JObject)newObj.DeepClone()));

                if (JToken.DeepEquals(normOld, normNew))
                {
                    result.ById[id] = ChangeType.Unchanged;
                    result.UnchangedIds.Add(id);
                }
                else
                {
                    result.ById[id] = ChangeType.Modified;
                    result.ModifiedIds.Add(id);
                }
            }
        }

        return result;
    }

    private Dictionary<string, JObject> LoadAndIndex(string path)
    {
        using (var sr = File.OpenText(path))
        using (var jr = new JsonTextReader(sr))
        {
            jr.DateParseHandling = DateParseHandling.None;
            var root = JToken.ReadFrom(jr);

            IEnumerable<JObject> objects;
            if (root is JArray)
            {
                objects = ((JArray)root).OfType<JObject>();
            }
            else if (root is JObject)
            {
                objects = ((JObject)root).Properties().Select(p => p.Value).OfType<JObject>();
            }
            else
            {
                throw new InvalidOperationException("Top-level JSON must be an array or an object.");
            }

            var dict = new Dictionary<string, JObject>(StringComparer.OrdinalIgnoreCase);
            foreach (var o in objects)
            {
                var idToken = o[_idField];
                if (idToken == null || idToken.Type == JTokenType.Null)
                    throw new InvalidOperationException("Object missing required id field '" + _idField + "'. Object: " + o);

                string id;
                if (idToken.Type == JTokenType.Integer || idToken.Type == JTokenType.Float)
                    id = idToken.ToString(); // normalize numeric ids to string
                else
                {
                    id = idToken.Value<string>();
                    if (id == null)
                        throw new InvalidOperationException("Invalid id for object: " + o);
                }

                if (dict.ContainsKey(id))
                    throw new InvalidOperationException("Duplicate id '" + id + "' encountered.");

                dict.Add(id, o);
            }

            return dict;
        }
    }

    private JObject RemoveIgnored(JObject obj)
    {
        foreach (var f in _ignoreFields)
        {
            obj.Remove(f);
        }
        return obj;
    }

    private JToken Normalize(JToken token)
    {
        // Recursively normalize objects (sort properties) and arrays (optionally sort by canonical string)
        if (token == null) return token;

        if (token.Type == JTokenType.Object)
        {
            var obj = (JObject)token;
            var props = obj.Properties()
                           .Select(p => new JProperty(p.Name, Normalize(p.Value)))
                           .OrderBy(p => p.Name, StringComparer.Ordinal)
                           .ToList();
            var sorted = new JObject();
            foreach (var p in props) sorted.Add(p);
            return sorted;
        }

        if (token.Type == JTokenType.Array)
        {
            var arr = (JArray)token;
            var normalizedItems = new List<JToken>();
            foreach (var t in arr) normalizedItems.Add(Normalize(t));

            if (_ignoreArrayOrder)
            {
                normalizedItems = normalizedItems
                    .OrderBy(t => t.ToString(Formatting.None), StringComparer.Ordinal)
                    .ToList();
            }
            return new JArray(normalizedItems);
        }

        // primitives are fine
        return token;
    }
}

public static class Program
{
    public static void Main(string[] args)
    {
        // Usage:
        // <exe> <old.json> <new.json> [idField=id] [ignoreArrayOrder=false] [ignoreFields=updatedAt,version]
        if (args.Length < 2)
        {
            Console.Error.WriteLine("Usage: <exe> <old.json> <new.json> [idField=id] [ignoreArrayOrder=false] [ignoreFields=updatedAt,version]");
            Environment.Exit(1);
        }

        var oldPath = args[0];
        var newPath = args[1];
        var idField = args.Length >= 3 ? args[2] : "id";

        bool ignoreArrayOrder = false;
        if (args.Length >= 4)
        {
            bool parsed;
            if (bool.TryParse(args[3], out parsed)) ignoreArrayOrder = parsed;
        }

        var ignoreFields = new string[0];
        if (args.Length >= 5 && !string.IsNullOrWhiteSpace(args[4]))
        {
            ignoreFields = args[4].Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < ignoreFields.Length; i++) ignoreFields[i] = ignoreFields[i].Trim();
        }

        var diff = new JsonDiffById(idField, ignoreArrayOrder, ignoreFields).CompareFiles(oldPath, newPath);

        Console.WriteLine("New: " + diff.NewIds.Count);
        Console.WriteLine("Deleted: " + diff.DeletedIds.Count);
        Console.WriteLine("Modified: " + diff.ModifiedIds.Count);
        Console.WriteLine("Unchanged: " + diff.UnchangedIds.Count);
        Console.WriteLine();

        PrintBucket("NEW", diff.NewIds);
        PrintBucket("DELETED", diff.DeletedIds);
        PrintBucket("MODIFIED", diff.ModifiedIds);
        PrintBucket("UNCHANGED", diff.UnchangedIds);
    }

    private static void PrintBucket(string title, IEnumerable<string> ids)
    {
        Console.WriteLine("== " + title + " ==");
        foreach (var id in ids.OrderBy(x => x, StringComparer.OrdinalIgnoreCase))
            Console.WriteLine(id);
        Console.WriteLine();
    }
}
