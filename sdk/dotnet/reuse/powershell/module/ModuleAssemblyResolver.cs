using System.Reflection;

namespace Dilock.PowerShell;

/// <summary>Resolves the module's private managed dependencies without entering a PowerShell runspace.</summary>
public static class ModuleAssemblyResolver
{
    private static readonly object Gate = new();
    private static readonly ResolveEventHandler Handler = Resolve;
    private static string? libraryDirectory;
    private static int registrations;

    /// <summary>Registers the directory containing the module's private dependency assemblies.</summary>
    public static void Register(string directory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        var fullPath = Path.GetFullPath(directory);
        lock (Gate)
        {
            if (libraryDirectory is not null && !string.Equals(libraryDirectory, fullPath, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Dilock PowerShell runtime assemblies are already registered from another directory.");

            libraryDirectory = fullPath;
            if (registrations++ == 0)
                AppDomain.CurrentDomain.AssemblyResolve += Handler;
        }
    }

    /// <summary>Unregisters one module import and removes the resolver after the final import is removed.</summary>
    public static void Unregister()
    {
        lock (Gate)
        {
            if (registrations == 0)
                return;

            if (--registrations == 0)
            {
                AppDomain.CurrentDomain.AssemblyResolve -= Handler;
                libraryDirectory = null;
            }
        }
    }

    private static Assembly? Resolve(object? sender, ResolveEventArgs args)
    {
        string? directory;
        lock (Gate) directory = libraryDirectory;
        if (directory is null)
            return null;

        var simpleName = new AssemblyName(args.Name).Name;
        if (string.IsNullOrEmpty(simpleName))
            return null;

        var candidate = Path.Combine(directory, simpleName + ".dll");
        return File.Exists(candidate) ? Assembly.LoadFrom(candidate) : null;
    }
}
