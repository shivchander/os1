import { RecoveryScreen } from "./RecoveryScreen";

export function RelaunchRequiredScreen() {
  return (
    <RecoveryScreen
      testId="relaunch-required"
      title="Restart OS1 to finish recovery"
      body="Your identity was updated. OS1 needs to restart so syncing and agents run under it."
    />
  );
}
