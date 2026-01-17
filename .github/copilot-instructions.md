# ADF Clone Extension - Copilot Instructions

This is a VS Code extension for Azure Data Factory/Synapse Pipeline UI clone.

## Project Structure
- Extension entry point: `src/extension.ts` - Registers commands and activates the extension
- Pipeline editor provider: `src/pipelineEditor.ts` - Manages webview with canvas-based UI
- Build output: `dist/` - Compiled JavaScript files
- TypeScript-based with esbuild bundler for fast compilation

## Development Guidelines
- Keep UI rendering using vanilla JavaScript/TypeScript with Canvas API
- Minimize external UI packages (zero UI dependencies currently)
- Focus on drag-and-drop pipeline canvas functionality
- Activity nodes, connections, and properties panel all built with Canvas API
- Uses VS Code CSS variables for theme compatibility

## Key Features Implemented
- ✅ Visual pipeline designer with drag-and-drop
- ✅ 9 activity types (Copy, Delete, Dataflow, Notebook, ForEach, IfCondition, Wait, WebActivity, StoredProcedure)
- ✅ Connection management with bezier curves
- ✅ Properties panel for editing activity details
- ✅ Canvas controls (zoom, pan, clear)
- ✅ Context menu for quick actions
- ✅ Grid background for alignment
- ✅ Beautiful UI matching VS Code theme

## How to Run
1. Press F5 to launch Extension Development Host
2. In the new window, press Ctrl+Shift+P
3. Type "ADF: Open Pipeline Editor" and press Enter

## Development Commands
- `npm run compile` - Compile TypeScript with type checking and linting
- `npm run watch` - Watch mode for development
- `npm test` - Run tests
- `F5` - Launch extension in debug mode

## Architecture
- Extension uses webview API to host the canvas editor
- Message passing between extension and webview for commands
- All rendering done with HTML5 Canvas 2D context
- Activities stored as objects with x, y coordinates
- Connections drawn using bezier curves

## Steps Completed
- [x] Create copilot-instructions.md
- [x] Scaffold VS Code extension with TypeScript
- [x] Create pipeline editor with Canvas API
- [x] Implement drag-and-drop functionality
- [x] Add connection management
- [x] Build properties panel
- [x] Add toolbar controls
- [x] Compile and test successfully
- [x] Update README and documentation

