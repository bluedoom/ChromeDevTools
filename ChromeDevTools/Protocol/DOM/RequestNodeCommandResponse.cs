using ChromeDevTools;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace ChromeDevTools.Protocol.DOM
{
	/// <summary>
	/// Requests that the node is sent to the caller given the JavaScript node object reference. All nodes that form the path from the node to the root are also sent to the client as a series of <code>setChildNodes</code> notifications.
	/// </summary>
	[CommandResponse(ProtocolName.DOM.RequestNode)]
	public class RequestNodeCommandResponse
	{
		/// <summary>
		/// Gets or sets Node id for given object.
		/// </summary>
		public long NodeId { get; set; }
	}
}