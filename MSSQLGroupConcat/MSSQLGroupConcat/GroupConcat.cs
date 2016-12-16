using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections.Generic;
using System.Text;
using System.IO;

[Serializable]
[Microsoft.SqlServer.Server.SqlUserDefinedAggregate(Format.UserDefined)]
public struct GroupConcat : IBinarySerialize
{
    private Dictionary<string, int> values;
    public void Init()
    {
        this.values = new Dictionary<string, int>();
    }

    public void Accumulate(SqlString Value)
    {
        if (!Value.IsNull)
        {
            string key = Value.Value;
            if (this.values.ContainsKey(key))
            {
                this.values[key] += 1;
            }
            else
            {
                this.values.Add(key, 1);
            }
        }
    }

    public void Merge(GroupConcat Group)
    {
        foreach (KeyValuePair<string, int> item in Group.values)
        {
            string key = item.Key;
            if (this.values.ContainsKey(key))
            {
                this.values[key] += Group.values[key];
            }
            else
            {
                this.values.Add(key, Group.values[key]);
            }
        }
    }

    public SqlString Terminate()
    {
        if (this.values != null && this.values.Count > 0)
        {
            StringBuilder sb = new StringBuilder();

            foreach (KeyValuePair<string, int> item in this.values)
            {
                for (int value = 0; value < item.Value; value++)
                {
                    sb.Append(item.Key);
                    sb.Append(",");
                }
            }
            return sb.Remove(sb.Length - 1, 1).ToString();
        }

        return null;
    }

    public void Read(BinaryReader r)
    {
        int itemCount = r.ReadInt32();
        this.values = new Dictionary<string, int>(itemCount);
        for (int i = 0; i <= itemCount - 1; i++)
        {
            this.values.Add(r.ReadString(), r.ReadInt32());
        }
    }

    public void Write(BinaryWriter w)
    {
        w.Write(this.values.Count);
        foreach (KeyValuePair<string, int> s in this.values)
        {
            w.Write(s.Key);
            w.Write(s.Value);
        }
    }
}
