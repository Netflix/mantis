# Mantis Testing Cheat Sheet

Quick reference for common testing commands and patterns.

## 🚀 Quick Commands

### Run Control Plane Tests
```bash
# All standard tests
./gradlew :mantis-control-plane:mantis-control-plane-server:test --rerun-tasks

# All Akka tests (single-threaded)
./gradlew :mantis-control-plane:mantis-control-plane-server:akkaTest --rerun-tasks

# Both suites
./gradlew :mantis-control-plane:mantis-control-plane-server:test :mantis-control-plane:mantis-control-plane-server:akkaTest --rerun-tasks

# Specific test class
./gradlew :mantis-control-plane:mantis-control-plane-server:test --tests "*.ResourceClusterActorTest" --rerun-tasks

# Specific test method
./gradlew :mantis-control-plane:mantis-control-plane-server:akkaTest --tests "*.JobClusterAkkaTest.testScaleStage" --rerun-tasks
```

## 🤖 Subagent Prompts (Copy-Paste)

### Basic
```
Run mantis-control-plane-server akkaTest and fix any failures
```

### Comprehensive
```
Use a subagent to run both test suites, analyze all failures, and create a detailed report
```

### Parallel
```
Run test and akkaTest in parallel using subagents, then summarize results
```

### Investigate
```
Investigate why testAssignmentTimeout is failing using a subagent
```

### Background
```
Run the full test suite in the background
```

## 📊 Check Results

```bash
# View HTML report
open mantis-control-plane/mantis-control-plane-server/build/reports/tests/test/index.html
open mantis-control-plane/mantis-control-plane-server/build/reports/tests/akkaTest/index.html

# Find failures in XML
find mantis-control-plane/mantis-control-plane-server/build/test-results -name "*.xml" -exec grep -l "failure" {} \;

# Extract failure details
grep -A 20 "<failure" mantis-control-plane/mantis-control-plane-server/build/test-results/test/TEST-*.xml
```

## 🔍 Common Issues & Fixes

### NullPointerException from Mocks
```java
// Add stub in test setup
when(gateway.submitTask(any())).thenReturn(CompletableFuture.completedFuture(Ack.getInstance()));
```

### Scheduler API Verification Failures
```java
// OLD (fails)
verify(schedulerMock).scheduleWorkers(any());

// NEW (correct)
verify(schedulerMock).upsertReservation(any());
```

### Assignment Timeout Test
```java
// For timeout tests, use hanging future
CompletableFuture<Ack> hangingFuture = new CompletableFuture<>();
when(gateway.submitTask(any())).thenReturn(hangingFuture);
```

## 📁 Key Files

| File | Purpose |
|------|---------|
| `JobTestHelper.java:535` | Mock scheduler setup |
| `ResourceClusterActorTest.java` | Main resource cluster tests |
| `JobClusterAkkaTest.java` | Job cluster Akka tests |
| `build.gradle` (root) | `akkaTest` task config |

## 🎯 Test Suites

| Suite | Count | Runtime | Notes |
|-------|-------|---------|-------|
| `test` | 330+ | ~2 min | Standard unit tests |
| `akkaTest` | 81+ | ~5 sec | Akka actor tests, single-threaded |

## 💡 Pro Tips

1. **Always use `--rerun-tasks`** to bypass Gradle cache
2. **Run specific tests first** when debugging
3. **Check HTML reports** for detailed failure info
4. **Use subagents** for multi-step workflows
5. **Run flaky tests multiple times** to confirm stability

## 🔗 See Also

- [Full Testing Guide](.claude/testing-guide.md) - Comprehensive testing documentation
- [Subagent Examples](.claude/subagent-examples.md) - Copy-paste subagent prompts
