# Intended Usage

<- [Back to README](../README.md)

---

Engram is persistent memory for AI coding agents. It commits durable results at
settled root-turn boundaries and recalls them across sessions. This page explains
the mental model rather than the API or architecture.

---

## If You Installed via gentle-ai -- You're Done

Engram is already configured. Your AI agent applies the canonical terminal
checkpoint policy and retrieves Memory when it can change the work.

You never need to configure it, inspect it, or interact with it directly. If engram is working correctly, you won't even notice it's there. That's the point.

---

## If You're Integrating Engram Into Your Agent

Install the plugin or use the preset. The canonical Memory protocol is included;
it owns terminal disposition and selective Recall.

You do not need to add a competing save or session-close policy. Optional Session
summaries and independent saves remain available through explicit curation.

That's it. There is no step two.

---

## The Golden Rule

Engram is infrastructure. Like a good database, you set it up once and forget about it.

If you're thinking about engram while working, something went wrong. It should be invisible.

---

## Quick Reference

| Do | Don't |
|----|-------|
| Install via gentle-ai, plugin, or preset | Manually inspect or edit engram's storage |
| Trust the canonical terminal checkpoint policy | Add a second disposition rubric |
| Just start coding -- Memory commits at settled turn boundaries | Treat host lifecycle events as Memory decisions |
| Re-run the installer to update | Worry about what's being saved or when |
