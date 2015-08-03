﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using Amazon.DynamoDBv2.DocumentModel;
using ServiceStack.Aws.Support;
using ServiceStack.DataAnnotations;

namespace ServiceStack.Aws.DynamoDb
{
    public static class DynamoUtils
    {
        public static Dictionary<string, DynamoDBEntry> ToDynamoDbEntryMap(this Type type)
        {
            return new Dictionary<string, DynamoDBEntry>();
        }

        public static Dictionary<Type, string> Set<T>(this Dictionary<Type, string> map, string value)
        {
            map[typeof(T)] = value;
            return map;
        }
    }

    public class DynamoMetadata
    {
        public static DynamoConverters Converters = new DynamoConverters();

        public static readonly Dictionary<Type, string> FieldTypeMap = new Dictionary<Type, string>()
            .Set<string>(DynamoType.String)
            .Set<bool>(DynamoType.Bool)
            .Set<byte[]>(DynamoType.Binary)
            .Set<Stream>(DynamoType.Binary)
            .Set<MemoryStream>(DynamoType.Binary)
            .Set<byte>(DynamoType.Number)
            .Set<sbyte>(DynamoType.Number)
            .Set<short>(DynamoType.Number)
            .Set<ushort>(DynamoType.Number)
            .Set<int>(DynamoType.Number)
            .Set<uint>(DynamoType.Number)
            .Set<long>(DynamoType.Number)
            .Set<ulong>(DynamoType.Number)
            .Set<float>(DynamoType.Number)
            .Set<double>(DynamoType.Number)
            .Set<decimal>(DynamoType.Number)
            .Set<HashSet<string>>(DynamoType.StringSet)
            .Set<HashSet<int>>(DynamoType.NumberSet)
            .Set<HashSet<long>>(DynamoType.NumberSet)
            .Set<HashSet<double>>(DynamoType.NumberSet)
            .Set<HashSet<float>>(DynamoType.NumberSet)
            .Set<HashSet<decimal>>(DynamoType.NumberSet);

        public static HashSet<DynamoMetadataType> Types;

        public static DynamoMetadataType GetTable<T>()
        {
            return GetTable(typeof(T));
        }

        public static DynamoMetadataType GetTable(Type table)
        {
            var metadata = Types.FirstOrDefault(x => x.Type == table);
            if (metadata == null || !metadata.IsTable)
                throw new ArgumentNullException("table", "Table has not been registered: " + table.Name);

            return metadata;
        }

        public static List<DynamoMetadataType> GetTables()
        {
            return Types == null 
                ? new List<DynamoMetadataType>() 
                : Types.Where(x => x.IsTable).ToList();
        }

        public static DynamoMetadataType GetType<T>()
        {
            return GetTable(typeof(T));
        }

        public static DynamoMetadataType GetType(Type type)
        {
            var metadata = Types.FirstOrDefault(x => x.Type == type);
            if (metadata != null)
                return metadata;

            RegisterTypes(type);

            return Types.First(x => x.Type == type);
        }

        public static void RegisterTables(IEnumerable<Type> tables)
        {
            foreach (var table in tables)
            {
                RegisterTable(table);
            }
        }

        // Should only be called at StartUp
        public static DynamoMetadataType RegisterTable(Type type)
        {
            if (Types == null)
                Types = new HashSet<DynamoMetadataType>();

            var table = Types.FirstOrDefault(x => x.Type == type);
            if (table != null)
            {
                table.IsTable = true;
                return table;
            }

            table = ToMetadataType(type);
            table.IsTable = true;
            Types.Add(table);

            LicenseUtils.AssertValidUsage(LicenseFeature.Aws, QuotaType.Tables, Types.Count);

            RegisterTypes(type.GetReferencedTypes());

            return table;
        }

        public static void RegisterTypes(params Type[] refTypes)
        {
            var metadatas = refTypes.Where(x => !x.IsValueType).Map(ToMetadataType);

            // Make thread-safe to allow usage at runtime
            HashSet<DynamoMetadataType> snapshot, newCache;
            do
            {
                snapshot = Types;
                newCache = new HashSet<DynamoMetadataType>(Types);
                foreach (var metadata in metadatas)
                {
                    newCache.Add(metadata);
                }
            } while (!ReferenceEquals(
                Interlocked.CompareExchange(ref Types, newCache, snapshot), snapshot));
        }

        private static DynamoMetadataType ToMetadataType(Type type)
        {
            var alias = type.FirstAttribute<AliasAttribute>();
            var props = type.GetSerializableProperties();
            PropertyInfo hash, range;
            Converters.GetHashAndRangeKeyFields(type, props, out hash, out range);

            var metadata = new DynamoMetadataType
            {
                Type = type,
                Name = alias != null ? alias.Name : type.Name,
            };
            metadata.Fields = props.Map(p =>
                new DynamoMetadataField
                {
                    Parent = metadata,
                    Type = p.PropertyType,
                    Name = Converters.GetFieldName(p),
                    DbType = Converters.GetFieldType(p.PropertyType),
                    IsHashKey = p == hash,
                    IsRangeKey = p == range,
                    IsAutoIncrement = p.HasAttribute<AutoIncrementAttribute>(),
                    SetValueFn = p.GetPropertySetterFn(),
                    GetValueFn = p.GetPropertyGetterFn(),
                }).ToArray();

            metadata.HashKey = metadata.Fields.FirstOrDefault(x => x.IsHashKey);
            metadata.RangeKey = metadata.Fields.FirstOrDefault(x => x.IsRangeKey);

            return metadata;
        }
    }


    public class DynamoMetadataField
    {
        public DynamoMetadataType Parent { get; set; }

        public Type Type { get; set; }

        public string Name { get; set; }

        public string DbType { get; set; }

        public bool IsHashKey { get; set; }

        public bool IsRangeKey { get; set; }

        public bool IsAutoIncrement { get; set; }

        public PropertyGetterDelegate GetValueFn { get; set; }

        public PropertySetterDelegate SetValueFn { get; set; }

        public object GetValue(object onInstance)
        {
            return this.GetValueFn == null ? null : this.GetValueFn(onInstance);
        }
    }

    public class DynamoMetadataType
    {
        public string Name { get; set; }

        public bool IsTable { get; set; }

        public Type Type { get; set; }

        public DynamoMetadataField[] Fields { get; set; }

        public DynamoMetadataField HashKey { get; set; }

        public DynamoMetadataField RangeKey { get; set; }
    }

}