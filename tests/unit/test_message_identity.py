def test_invocation_identity_keeps_three_chunks_and_completion_together():
    from agentseek_api.services.message_identity import MessageIdentityTracker
    tracker = MessageIdentityTracker()
    def resolve(invocation, namespace=(), index=0, provider=None, complete=False):
        return tracker.resolve(invocation_id=invocation, namespace=namespace,
            provider_message_id=provider, message_index=index, complete=complete)
    first = resolve("call-a")
    assert [resolve("call-a") for _ in range(3)] == [first] * 3
    second = resolve("call-b")
    assert second != first and resolve("call-a", complete=True) == first
    assert resolve("call-a", ("child",)) not in {first, second}
    assert resolve("call-a", index=1) != first
    assert resolve("call-c", provider="provider-message") == "provider-message"
    # A late provider ID cannot split a message already visible to clients.
    assert resolve("call-a", provider="late-id") == first
