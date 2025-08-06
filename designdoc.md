# Target

Given a configuration model, generate as many variants as possible and validate that they complete without errors, produce a final price, and output a complete BOM.

---

# High Level Design

The program requires the available options/fields to be able to generate new variants. The only way the options can be easily obtained is through the interactive generator, so the following process is used to load a model for generation:

1. Push model into **SwiftSell**
2. Open the network inspector
3. Complete a configuration
4. Open the last `configure` request and download the JSON to a new file

> **Note:**
> If some options are locked behind a certain choice (e.g., certain colors only available for certain models), those options will not be in the file and a new configuration will have to be made.

Once a model’s JSON is obtained, it can then be used with the program, which will generate variants and write them to an SQLite database.

The program can generate invalid variants even if the model is perfect, because it has no access to groups and constraints.

From there, a **sender** can be instantiated, which will asynchronously run some operation on each variant (e.g., sending to a server, running a command, or writing to a database). This behavior can be customized with the `runner` object.

The response sent back should indicate the **validity** of the variant/operation, and it will be written to the SQLite database as well.

> **Note:**
> Verification of variants hasn’t been implemented, as Infor does not offer a clear way to validate variants through an API. A C# wrapper will likely be required, as the only known way to run a configuration is through legacy .NET Framework C# code.

---

# Implementation

* Repository: [https://github.com/benrentech/configuration-tester](https://github.com/benrentech/configuration-tester)
* Language: **Python**
* Main files:

  * `producer`: creates all possible variants and writes them to a database
  * `consumer`: reads variants from the database, sends them to a URL to validate, and writes results back
  * `runner`: defines what action is taken when a variant is processed by the sender

On the sender side, many requests are required. Due to the volume, concurrency is necessary. The solution uses `asyncio`, the standard for I/O concurrency in Python.

There are separate asynchronous loops for:

* Reading from the database
* Writing to the database
* Worker processes (typically 10–50)

> `producer` is completely synchronous.

---

# Challenges & Failure

### Attempt 1: Python → C# DLL (via pythonnet)

* Tried calling the C# DLL directly from Python using `pythonnet`
* Failed due to SSL connection errors — the DLL could not connect to Infor servers when called this way

### Attempt 2: Port to .NET Core

* Tried converting the legacy .NET Framework C# code to **.NET Core**
* Unfeasible: the code depended on features (e.g., **WCF**) not supported in .NET Core

### Attempt 3: .NET Core API wrapper

* Tried wrapping the .NET Framework code with a .NET Core Web API
* Still required .NET Framework code to run
* The wrapper added no value and didn’t resolve the compatibility issues