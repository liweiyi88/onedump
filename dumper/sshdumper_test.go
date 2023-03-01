package dumper

import "testing"

func TestEnsureSSHHostHavePort(t *testing.T) {
	sshHost := "127.0.0.1"

	if ensureHaveSSHPort(sshHost) != sshHost+":22" {
		t.Error("ssh host port is not ensured")
	}

	sshHost = "127.0.0.1:22"
	actual := ensureHaveSSHPort(sshHost)
	if actual != sshHost {
		t.Errorf("expect ssh host: %s, actual: %s", sshHost, actual)
	}
}
