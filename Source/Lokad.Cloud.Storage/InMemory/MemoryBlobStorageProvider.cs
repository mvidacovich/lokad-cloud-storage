#region Copyright (c) Lokad 2009-2011
// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Lokad.Cloud.Storage.InMemory
{
    /// <summary>Mock in-memory Blob Storage.</summary>
    /// <remarks>
    /// All the methods of <see cref="MemoryBlobStorageProvider"/> are thread-safe.
    /// Note that the blob lease implementation is simplified such that leases do not time out.
    /// </remarks>
    public class MemoryBlobStorageProvider : IBlobStorageProvider
    {
        /// <summary> Containers Property.</summary>
        Dictionary<string, MemoryContainer> Containers { get { return _containers;} }
        readonly Dictionary<string, MemoryContainer> _containers;
        
        /// <summary>naive global lock to make methods thread-safe.</summary>
        readonly object _syncRoot;

        internal IDataSerializer DefaultSerializer { get; set; }

        public MemoryBlobStorageProvider(IDataSerializer defaultSerializer = null)
        {
            _containers = new Dictionary<string, MemoryContainer>();
            _syncRoot = new object();
            DefaultSerializer = defaultSerializer ?? new CloudFormatter();
        }

        public IEnumerable<string> ListContainers(string prefix = null)
        {
            lock (_syncRoot)
            {
                if (String.IsNullOrEmpty(prefix))
                {
                    return Containers.Keys;
                }

                return Containers.Keys.Where(key => key.StartsWith(prefix));
            }
        }

        public bool CreateContainerIfNotExist(string containerName)
        {
            lock (_syncRoot)
            {
                if (!BlobStorageExtensions.IsContainerNameValid(containerName))
                {
                    throw new NotSupportedException("the containerName is not compliant with azure constraints on container names");
                }

                if (Containers.Keys.Contains(containerName))
                {
                    return false;
                }
                
                Containers.Add(containerName, new MemoryContainer());
                return true;
            }	
        }

        public bool DeleteContainerIfExist(string containerName)
        {
            lock (_syncRoot)
            {
                if (!Containers.Keys.Contains(containerName))
                {
                    return false;
                }

                Containers.Remove(containerName);
                return true;
            }
        }

        public IEnumerable<string> ListBlobNames(string containerName, string blobNamePrefix = null)
        {
            lock (_syncRoot)
            {
                if (!Containers.Keys.Contains(containerName))
                {
                    return Enumerable.Empty<string>();
                }

                var names = Containers[containerName].BlobNames;
                return String.IsNullOrEmpty(blobNamePrefix) ? names : names.Where(name => name.StartsWith(blobNamePrefix));
            }
        }

        public IEnumerable<T> ListBlobs<T>(string containerName, string blobNamePrefix = null, int skip = 0, IDataSerializer serializer = null)
        {
            var names = ListBlobNames(containerName, blobNamePrefix);

            if (skip > 0)
            {
                names = names.Skip(skip);
            }

            return names.Select(name => GetBlob<T>(containerName, name, serializer))
                .Where(blob => blob.HasValue)
                .Select(blob => blob.Value);
        }

        public bool DeleteBlobIfExist(string containerName, string blobName)
        {
            lock (_syncRoot)
            {
                if (!Containers.Keys.Contains(containerName) || !Containers[containerName].BlobNames.Contains(blobName))
                {
                    return false;
                }

                Containers[containerName].RemoveBlob(blobName);
                return true;
            }
        }

        public void DeleteAllBlobs(string containerName, string blobNamePrefix = null)
        {
            foreach (var blobName in ListBlobNames(containerName, blobNamePrefix))
            {
                DeleteBlobIfExist(containerName, blobName);
            }
        }

        public Maybe<T> GetBlob<T>(string containerName, string blobName, IDataSerializer serializer = null)
        {
            string ignoredEtag;
            return GetBlob<T>(containerName, blobName, out ignoredEtag, serializer);
        }

        public Maybe<T> GetBlob<T>(string containerName, string blobName, out string etag, IDataSerializer serializer = null)
        {
            return GetBlob(containerName, blobName, typeof(T), out etag, serializer)
                .Convert(o => o is T ? (T)o : Maybe<T>.Empty, Maybe<T>.Empty);
        }

        public Maybe<object> GetBlob(string containerName, string blobName, Type type, out string etag, IDataSerializer serializer = null)
        {
            return GetBlobStream(containerName, blobName, out etag).Convert(stream =>
            {
                using (stream)
                {
                    return (serializer ?? DefaultSerializer).Deserialize(stream, type);
                }
            });
        }

        public Maybe<Stream> GetBlobStream(string containerName, string blobName, out string etag)
        {
            lock (_syncRoot)
            {
                if (!Containers.ContainsKey(containerName) || !Containers[containerName].BlobNames.Contains(blobName))
                {
                    etag = null;
                    return Maybe<Stream>.Empty;
                }

                etag = Containers[containerName].BlobsEtag[blobName];
                return Containers[containerName].GetBlob(blobName);
            }
        }

        public Maybe<Stream> GetBlobOffsetStream(string containerName, string blobName, long offsetBytes, long lengthBytes, out string etag)
        {
            lock (_syncRoot)
            {
                if (!Containers.ContainsKey(containerName) || !Containers[containerName].BlobNames.Contains(blobName))
                {
                    etag = null;
                    return Maybe<Stream>.Empty;
                }

                etag = Containers[containerName].BlobsEtag[blobName];
                var stream = Containers[containerName].GetBlob(blobName);
                stream.Position = offsetBytes;
                var returnStream = new MemoryStream();
                byte[] buffer = new byte[8192];
                int bytesRead = 1;
                while (lengthBytes > 0 && bytesRead > 0)
                {
                    bytesRead = stream.Read(buffer, 0, Math.Min((int)lengthBytes, buffer.Length));
                    returnStream.Write(buffer, 0, bytesRead);
                    lengthBytes -= bytesRead;
                }

                returnStream.Position = 0;
                return new Maybe<Stream>(returnStream);
            }
        }

        public Task<BlobWithETag<object>> GetBlobAsync(string containerName, string blobName, Type type, CancellationToken cancellationToken, IDataSerializer serializer = null)
        {
            return TaskAsyncHelper.FromMethod(
                () =>
                {
                    string etag;
                    var blob = GetBlob(containerName, blobName, type, out etag, serializer);
                    return blob.Convert(o => new BlobWithETag<object> {Blob = o, ETag = etag}, () => default(BlobWithETag<object>));
                });
        }

        public Task<BlobWithETag<Stream>> GetBlobStreamAsync(string containerName, string blobName, CancellationToken cancellationToken)
        {
            return TaskAsyncHelper.FromMethod(
                () =>
                {
                    string etag;
                    var blob = GetBlobStream(containerName, blobName, out etag);
                    return blob.Convert(o => new BlobWithETag<Stream> {Blob = o, ETag = etag}, () => default(BlobWithETag<Stream>));
                });
        }

        public Maybe<XElement> GetBlobXml(string containerName, string blobName, out string etag, IDataSerializer serializer = null)
        {
            etag = null;

            var formatter = (serializer ?? DefaultSerializer) as IIntermediateDataSerializer;
            if (formatter == null)
            {
                return Maybe<XElement>.Empty;
            }

            MemoryStream stream;
            lock (_syncRoot)
            {
                if (!Containers.ContainsKey(containerName)
                    || !Containers[containerName].BlobNames.Contains(blobName))
                {
                    return Maybe<XElement>.Empty;
                }

                etag = Containers[containerName].BlobsEtag[blobName];
                stream = Containers[containerName].GetBlob(blobName);
            }

            stream.Position = 0;
            return formatter.UnpackXml(stream);
        }

        /// <remarks></remarks>
        public Maybe<T>[] GetBlobRange<T>(string containerName, string[] blobNames, out string[] etags, IDataSerializer serializer = null)
        {
            var tempResult = blobNames.Select(blobName =>
            {
                string etag;
                var blob = GetBlob<T>(containerName, blobName, out etag);
                return new Tuple<Maybe<T>, string>(blob, etag);
            }).ToArray();

            etags = new string[blobNames.Length];
            var result = new Maybe<T>[blobNames.Length];

            for (int i = 0; i < tempResult.Length; i++)
            {
                result[i] = tempResult[i].Item1;
                etags[i] = tempResult[i].Item2;
            }

            return result;
        }

        public Maybe<T> GetBlobIfModified<T>(string containerName, string blobName, string oldEtag, out string newEtag, IDataSerializer serializer = null)
        {
            lock (_syncRoot)
            {
                string currentEtag = GetBlobEtag(containerName, blobName);
                if (currentEtag == oldEtag)
                {
                    newEtag = null;
                    return Maybe<T>.Empty;
                }

                newEtag = currentEtag;
                return GetBlob<T>(containerName, blobName, serializer);
            }
        }

        public Maybe<Stream> GetBlobStreamIfModified(string containerName, string blobName, string oldEtag, out string newEtag)
        {
            lock (_syncRoot)
            {
                string currentEtag = GetBlobEtag(containerName, blobName);
                if (currentEtag == oldEtag)
                {
                    newEtag = null;
                    return Maybe<Stream>.Empty;
                }

                return GetBlobStream(containerName, blobName, out newEtag);
            }
        }

        public string GetBlobEtag(string containerName, string blobName)
        {
            lock (_syncRoot)
            {
                return (Containers.ContainsKey(containerName) && Containers[containerName].BlobNames.Contains(blobName))
                    ? Containers[containerName].BlobsEtag[blobName]
                    : null;
            }
        }

        public Task<string> GetBlobEtagAsync(string containerName, string blobName, CancellationToken cancellationToken)
        {
            return TaskAsyncHelper.FromMethod(() => GetBlobEtag(containerName, blobName));
        }

        public void PutBlob<T>(string containerName, string blobName, T item, IDataSerializer serializer = null)
        {
            string ignored;
            PutBlob(containerName, blobName, item, typeof(T), true, null, out ignored, serializer);
        }

        public bool PutBlob<T>(string containerName, string blobName, T item, bool overwrite, IDataSerializer serializer = null)
        {
            string ignored;
            return PutBlob(containerName, blobName, item, typeof(T), overwrite, null, out ignored, serializer);
        }

        public bool PutBlob<T>(string containerName, string blobName, T item, bool overwrite, out string etag, IDataSerializer serializer = null)
        {
            return PutBlob(containerName, blobName, item, typeof(T), overwrite, null, out etag, serializer);
        }

        public bool PutBlob<T>(string containerName, string blobName, T item, string expectedEtag, IDataSerializer serializer = null)
        {
            string ignored;
            return PutBlob(containerName, blobName, item, typeof (T), true, expectedEtag, out ignored, serializer);
        }

        public bool PutBlob(string containerName, string blobName, object item, Type type, bool overwrite, out string etag, IDataSerializer serializer = null)
        {
            return PutBlob(containerName, blobName, item, type, overwrite, null, out etag, serializer);
        }

        public bool PutBlobStream(string containerName, string blobName, Stream stream, bool overwrite, string expectedEtag, out string etag)
        {
            var memoryStream = stream as MemoryStream;
            if (memoryStream == null)
            {
                memoryStream = new MemoryStream();
                stream.CopyTo(memoryStream);
            }
            memoryStream.Position = 0;

            lock (_syncRoot)
            {
                etag = null;
                if (Containers.ContainsKey(containerName))
                {
                    if (Containers[containerName].BlobNames.Contains(blobName))
                    {
                        if (!overwrite || expectedEtag != null && expectedEtag != Containers[containerName].BlobsEtag[blobName])
                        {
                            return false;
                        }

                        Containers[containerName].SetBlob(blobName, memoryStream);
                        etag = Containers[containerName].BlobsEtag[blobName];
                        return true;
                    }

                    Containers[containerName].AddBlob(blobName, memoryStream);
                    etag = Containers[containerName].BlobsEtag[blobName];
                    return true;
                }

                if (!BlobStorageExtensions.IsContainerNameValid(containerName))
                {
                    throw new NotSupportedException("the containerName is not compliant with azure constraints on container names");
                }

                Containers.Add(containerName, new MemoryContainer());

                Containers[containerName].AddBlob(blobName, memoryStream);
                etag = Containers[containerName].BlobsEtag[blobName];
                return true;
            }
        }

        public bool PutBlob(string containerName, string blobName, object item, Type type, bool overwrite, string expectedEtag, out string etag, IDataSerializer serializer = null)
        {
            var dataSerializer = serializer ?? DefaultSerializer;
            lock(_syncRoot)
            {
                etag = null;
                if(Containers.ContainsKey(containerName))
                {
                    if(Containers[containerName].BlobNames.Contains(blobName))
                    {
                        if(!overwrite || expectedEtag != null && expectedEtag != Containers[containerName].BlobsEtag[blobName])
                        {
                            return false;
                        }

                        using (var stream = new MemoryStream())
                        {
                            dataSerializer.Serialize(item, stream, type);
                            Containers[containerName].SetBlob(blobName, stream);
                        }

                        etag = Containers[containerName].BlobsEtag[blobName];
                        return true;
                    }

                    using (var stream = new MemoryStream())
                    {
                        dataSerializer.Serialize(item, stream, type);
                        Containers[containerName].AddBlob(blobName, stream);
                    }

                    etag = Containers[containerName].BlobsEtag[blobName];
                    return true;
                }

                if (!BlobStorageExtensions.IsContainerNameValid(containerName))
                {
                    throw new NotSupportedException("the containerName is not compliant with azure constraints on container names");
                }

                Containers.Add(containerName, new MemoryContainer());

                using (var stream = new MemoryStream())
                {
                    dataSerializer.Serialize(item, stream, type);
                    Containers[containerName].AddBlob(blobName, stream);
                }

                etag = Containers[containerName].BlobsEtag[blobName];
                return true;
            }
        }

        public Task<string> PutBlobAsync(string containerName, string blobName, object item, Type type, bool overwrite, string expectedEtag, CancellationToken cancellationToken, IDataSerializer serializer = null)
        {
            return TaskAsyncHelper.FromMethod(
                () =>
                {
                    string etag;
                    PutBlob(containerName, blobName, item, type, overwrite, expectedEtag, out etag, serializer);
                    return etag;
                });
        }

        public Task<string> PutBlobStreamAsync(string containerName, string blobName, Stream stream, bool overwrite, string expectedEtag, CancellationToken cancellationToken)
        {
            return TaskAsyncHelper.FromMethod(
                () =>
                {
                    string etag;
                    PutBlobStream(containerName, blobName, stream, overwrite, expectedEtag, out etag);
                    return etag;
                });
        }

        public Maybe<T> UpdateBlobIfExist<T>(string containerName, string blobName, Func<T, T> update, IDataSerializer serializer = null)
        {
            return UpsertBlobOrSkip(containerName, blobName, () => Maybe<T>.Empty, t => update(t), serializer);
        }

        public Maybe<T> UpdateBlobIfExistOrSkip<T>(string containerName, string blobName, Func<T, Maybe<T>> update, IDataSerializer serializer = null)
        {
            return UpsertBlobOrSkip(containerName, blobName, () => Maybe<T>.Empty, update, serializer);
        }

        public Maybe<T> UpdateBlobIfExistOrDelete<T>(string containerName, string blobName, Func<T, Maybe<T>> update, IDataSerializer serializer = null)
        {
            var result = UpsertBlobOrSkip(containerName, blobName, () => Maybe<T>.Empty, update, serializer);
            if (!result.HasValue)
            {
                DeleteBlobIfExist(containerName, blobName);
            }

            return result;
        }

        public T UpsertBlob<T>(string containerName, string blobName, Func<T> insert, Func<T, T> update, IDataSerializer serializer = null)
        {
            return UpsertBlobOrSkip<T>(containerName, blobName, () => insert(), t => update(t), serializer).Value;
        }

        public Maybe<T> UpsertBlobOrSkip<T>(string containerName, string blobName, Func<Maybe<T>> insert, Func<T, Maybe<T>> update, IDataSerializer serializer = null)
        {
            lock (_syncRoot)
            {
                Maybe<T> input;
                if (Containers.ContainsKey(containerName))
                {
                    if (Containers[containerName].BlobNames.Contains(blobName))
                    {
                        using (var stream = Containers[containerName].GetBlob(blobName))
                        {
                            input = (T)(serializer ?? DefaultSerializer).Deserialize(stream, typeof(T));
                        }
                    }
                    else
                    {
                        input = Maybe<T>.Empty;
                    }
                }
                else
                {
                    Containers.Add(containerName, new MemoryContainer());
                    input = Maybe<T>.Empty;
                }

                var output = input.HasValue ? update(input.Value) : insert();

                if (output.HasValue)
                {
                    using (var stream = new MemoryStream())
                    {
                        (serializer ?? DefaultSerializer).Serialize(output.Value, stream, typeof(T));
                        Containers[containerName].SetBlob(blobName, stream);
                    }
                }

                return output;
            }
        }

        public Task<BlobWithETag<T>> UpsertBlobOrSkipAsync<T>(string containerName, string blobName,
            Func<Maybe<T>> insert, Func<T, Maybe<T>> update, CancellationToken cancellationToken, IDataSerializer serializer = null)
        {
            lock (_syncRoot)
            {
                return GetBlobAsync(containerName, blobName, typeof(T), cancellationToken, serializer)
                    .Then(b =>
                    {
                        var output = (b == null) ? insert() : update((T)b.Blob);
                        if (!output.HasValue)
                        {
                            return TaskAsyncHelper.FromResult(default(BlobWithETag<T>));
                        }

                        var putTask = (b == null)
                            ? PutBlobAsync(containerName, blobName, output.Value, typeof(T), false, null, cancellationToken, serializer)
                            : PutBlobAsync(containerName, blobName, output.Value, typeof(T), true, b.ETag, cancellationToken, serializer);
                        return putTask.Then(etag => new BlobWithETag<T> { Blob = output.Value, ETag = etag });
                    });
            }
        }

        public Maybe<T> UpsertBlobOrDelete<T>(string containerName, string blobName, Func<Maybe<T>> insert, Func<T, Maybe<T>> update, IDataSerializer serializer = null)
        {
            var result = UpsertBlobOrSkip(containerName, blobName, insert, update, serializer);
            if (!result.HasValue)
            {
                DeleteBlobIfExist(containerName, blobName);
            }

            return result;
        }

        class MemoryContainer
        {
            readonly Dictionary<string, byte[]> _blobSet;
            readonly Dictionary<string, string> _blobsEtag;
            readonly Dictionary<string, string> _blobsLeases;

            public string[] BlobNames { get { return _blobSet.Keys.ToArray(); } }

            public Dictionary<string, string> BlobsEtag { get { return _blobsEtag; } }
            public Dictionary<string, string> BlobsLeases { get { return _blobsLeases; } }

            public MemoryContainer()
            {
                _blobSet = new Dictionary<string, byte[]>();
                _blobsEtag = new Dictionary<string, string>();
                _blobsLeases = new Dictionary<string, string>();
            }

            public void SetBlob(string blobName, MemoryStream stream)
            {
                stream.Position = 0;
                _blobSet[blobName] = stream.ToArray();
                _blobsEtag[blobName] = Guid.NewGuid().ToString();
            }

            public MemoryStream GetBlob(string blobName)
            {
                var stream = new MemoryStream();
                stream.Write(_blobSet[blobName], 0, _blobSet[blobName].Length);
                stream.Position = 0;
                return stream;
            }

            public void AddBlob(string blobName, MemoryStream stream)
            {
                stream.Position = 0;
                _blobSet.Add(blobName, stream.ToArray());
                _blobsEtag.Add(blobName, Guid.NewGuid().ToString());
            }

            public void RemoveBlob(string blobName)
            {
                _blobSet.Remove(blobName);
                _blobsEtag.Remove(blobName);
                _blobsLeases.Remove(blobName);
            }
        }

        public bool IsBlobLocked(string containerName, string blobName)
        {
            lock (_syncRoot)
            {
                return (Containers.ContainsKey(containerName)
                    && Containers[containerName].BlobNames.Contains(blobName))
                    && Containers[containerName].BlobsLeases.ContainsKey(blobName);
            }
        }

        public Result<string> TryAcquireLease(string containerName, string blobName)
        {
            lock (_syncRoot)
            {
                if (!Containers[containerName].BlobsLeases.ContainsKey(blobName))
                {
                    var leaseId = Guid.NewGuid().ToString("N");
                    Containers[containerName].BlobsLeases[blobName] = leaseId;
                    return Result.CreateSuccess(leaseId);
                }

                return Result<string>.CreateError("Conflict");
            }
        }

        public Result<string> TryReleaseLease(string containerName, string blobName, string leaseId)
        {
            lock (_syncRoot)
            {
                string actualLeaseId;
                if (!Containers[containerName].BlobsLeases.TryGetValue(blobName, out actualLeaseId))
                {
                    return Result<string>.CreateError("NotFound");
                }
                if (actualLeaseId != leaseId)
                {
                    return Result<string>.CreateError("Conflict");
                }

                Containers[containerName].BlobsLeases.Remove(blobName);
                return Result.CreateSuccess("OK");
            }
        }

        public Result<string> TryRenewLease(string containerName, string blobName, string leaseId)
        {
            lock (_syncRoot)
            {
                string actualLeaseId;
                if (!Containers[containerName].BlobsLeases.TryGetValue(blobName, out actualLeaseId))
                {
                    return Result<string>.CreateError("NotFound");
                }
                if (actualLeaseId != leaseId)
                {
                    return Result<string>.CreateError("Conflict");
                }

                return Result.CreateSuccess("OK");
            }
        }
    }
}
