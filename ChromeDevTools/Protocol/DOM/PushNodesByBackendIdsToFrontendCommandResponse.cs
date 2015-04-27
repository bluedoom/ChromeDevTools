using ChromeDevTools;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace ChromeDevTools.Protocol.DOM
{
	/// <summary>
	/// Requests that a batch of nodes is sent to the caller given their backend node ids.
	/// </summary>
	[CommandResponse(ProtocolName.DOM.PushNodesByBackendIdsToFrontend)]
	public class PushNodesByBackendIdsToFrontendCommandResponse
	{
		/// <summary>
		/// Gets or sets The array of ids of pushed nodes that correspond to the backend ids specified in backendNodeIds.
		/// </summary>
		public long[] NodeIds { get; set; }
	}
}